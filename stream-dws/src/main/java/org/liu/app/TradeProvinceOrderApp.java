package org.liu.app;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.*;
import org.liu.bean.TradeProvinceOrderBean;
import org.liu.common.app.AppBase;
import org.liu.common.service.HBaseService;
import org.liu.common.util.HBaseConnectionUtil;
import org.liu.common.util.RedisUtil;
import org.liu.common.util.StreamUtil;
import redis.clients.jedis.Jedis;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.*;
import static org.liu.common.constant.Constant.*;

public class TradeProvinceOrderApp extends AppBase {
    private Connection conn;

    public static void main(String[] args) {
        new TradeProvinceOrderApp().start(args, 4);
    }

    @Override
    public void etl(SparkSession spark, String[] args) {
        Dataset<Row> source = deltaTableStream(spark, DWD_ORDER_DETAIL)
                .select("order_id", "province_id", "order_price", "create_time")
                .na().drop(new String[]{"province_id", "order_id", "create_time"});

        source = source.withWatermark("create_time", "1 seconds")
                .groupBy(col("province_id"),
                        window(col("create_time"), "10 seconds"))
                .agg(approx_count_distinct(col("order_id")).as("total_order_num"), sum("order_price").as("total_order_price"))
                .withColumn("startTime", col("window.start"))
                .withColumn("endTime", col("window.end"));

        /*
        Connect with dim_sku_info,dim_spu_info,dim_base_category3,dim_base_category2,dim_base_category1,dim_base_trademark
        They are stored in HBase for this testing
        Use Redis for in-memory database to decrease heavy connections with HBase and increase performance
        Here the connection with external systems is synchronous, Structured streaming cannot support async IO now
         */
        try {
            source.writeStream()
                    .option("checkpointLocation", StreamUtil.getTableCheckpointPath(DWS_LAYER, DWS_TRADE_PROVINCE_ORDER))
                    .foreachBatch(this::process)
                    .start();
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    private void process(Dataset<Row> src, long id) {
        Dataset<TradeProvinceOrderBean> data = src.mapPartitions((MapPartitionsFunction<Row, TradeProvinceOrderBean>) rows -> {
            Jedis client = RedisUtil.getClient();
            ArrayList<TradeProvinceOrderBean> res = new ArrayList<>();

            while (rows.hasNext()) {
                Row row = rows.next();
                String provinceId = row.getAs("province_id");
                Map<String, String> info = getInfo(DIM_BASE_PROVINCE, provinceId, new String[]{"name"}, client);

                TradeProvinceOrderBean bean = new TradeProvinceOrderBean();
                bean.setProvinceId(provinceId);
                bean.setStartTime(row.getAs("startTime"));
                bean.setEndTime(row.getAs("endTime"));
                bean.setDate(new SimpleDateFormat("yyyy-MM-dd").format(bean.getEndTime()));
                bean.setTotalOrderNum(row.getAs("total_order_num"));
                bean.setTotalOrderPrice(row.getAs("total_order_price"));
                bean.setProvinceName(info.get("name"));

                res.add(bean);
            }

            HBaseConnectionUtil.closeConnection(conn);
            RedisUtil.closeClient(client);

            return res.iterator();
        }, Encoders.bean(TradeProvinceOrderBean.class));

        if (!data.isEmpty()) {
            data.write().format("delta")
                    .mode(SaveMode.Append)
                    .partitionBy("date")
                    .option("path", StreamUtil.getTablePath(DWS_LAYER, DWS_TRADE_PROVINCE_ORDER))
                    .saveAsTable(DELTA_DB + "." + DWS_TRADE_PROVINCE_ORDER);
            data.show();
        }
    }

    private Map<String, String> getInfo(String table, String id, String[] columns, Jedis client) {
        Map<String, String> info;
        String key = RedisUtil.getKey(table, id);
        // Read HBase if redis not cached the row
        if (!client.exists(key)) {
            if (conn == null) conn = HBaseConnectionUtil.newConnection();
            HBaseService service = new HBaseService(conn);
            info = service.getColumns(DATABASE, table, id, "info", columns);
            info.forEach((k, v) -> client.hset(key, k, v));
            client.expire(key, 24 * 60 * 60);
        } else {
            // Read redis cache to avoid to create heavy HBase connection and expedite row fetching
            // But if some columns are not cached, have to read from HBase and cache them
            info = client.hgetAll(key);
            for (String column : columns) {
                if (!info.containsKey(column)) {
                    if (conn == null) conn = HBaseConnectionUtil.newConnection();
                    HBaseService service = new HBaseService(conn);
                    String value = service.getColumn(DATABASE, table, id, "info", column);
                    info.put(column, value);
                    client.hset(key, column, value);
                }
            }
        }
        return info;
    }
}

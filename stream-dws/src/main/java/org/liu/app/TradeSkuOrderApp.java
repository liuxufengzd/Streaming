package org.liu.app;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.json.JSONObject;
import org.liu.bean.TradeSkuOrderBean;
import org.liu.common.app.AppBase;
import org.liu.common.service.HBaseService;
import org.liu.common.util.HBaseConnectionUtil;
import org.liu.common.util.RedisUtil;
import org.liu.common.util.StreamUtil;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.*;
import static org.liu.common.constant.Constant.*;

public class TradeSkuOrderApp extends AppBase {
    public static void main(String[] args) {
        new TradeSkuOrderApp().start(args, 4);
    }

    @Override
    public void etl(SparkSession spark, String[] args) {
        Dataset<Row> source = deltaTableStream(spark, DWD_ORDER_DETAIL)
                .select("sku_id", "sku_name", "sku_num", "order_price", "create_time")
                .na().drop(new String[]{"sku_id", "create_time"});

        source = source.withWatermark("create_time", "1 seconds")
                .groupBy(col("sku_id"),
                        window(col("create_time"), "5 seconds"))
                .agg(sum("sku_num").as("total_sku_num"), sum("order_price").as("total_order_price"))
                .withColumn("startTime", col("window.start"))
                .withColumn("endTime", col("window.end"));

        /*
        Connect with dim_sku_info,dim_spu_info,dim_base_category3,dim_base_category2,dim_base_category1,dim_base_trademark
        They are stored in HBase for this testing
        Use Redis for in-memory database to decrease heavy connections with HBase and increase performance
         */
        try {
            source.writeStream()
                    .option("checkpointLocation", StreamUtil.getTableCheckpointPath(DWS_LAYER, DWS_TRADE_SKU_ORDER))
                    .foreachBatch(this::process)
                    .start();
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    private void process(Dataset<Row> src, long id) {
        Dataset<TradeSkuOrderBean> data = src.mapPartitions((MapPartitionsFunction<Row, TradeSkuOrderBean>) rows -> {
            Connection conn = null;
            Jedis client = RedisUtil.getClient();
            ArrayList<TradeSkuOrderBean> res = new ArrayList<>();

            while (rows.hasNext()) {
                Row row = rows.next();
                String skuId = row.getAs("sku_id");
                JSONObject dim;
                if (client.exists(skuId)) {
                    dim = new JSONObject(client.get(skuId));
                } else {
                    if (conn == null) conn = HBaseConnectionUtil.newConnection();
                    HBaseService service = new HBaseService(conn);
                    Map<String, String> skuInfo = service.getColumns(DATABASE, DIM_SKU_INFO, skuId, "info.category3_id,info.tm_id,info.spu_id,info.sku_name");
                    Map<String, String> spuInfo = service.getColumns(DATABASE, DIM_SPU_INFO, skuInfo.get("spu_id"), "info.spu_name");
                    Map<String, String> baseCat3 = service.getColumns(DATABASE, DIM_BASE_CATEGORY3, skuInfo.get("category3_id"), "info.name,info.category2_id");
                    Map<String, String> baseCat2 = service.getColumns(DATABASE, DIM_BASE_CATEGORY2, baseCat3.get("category2_id"), "info.name,info.category1_id");
                    Map<String, String> baseCat1 = service.getColumns(DATABASE, DIM_BASE_CATEGORY1, baseCat2.get("category1_id"), "info.name");
                    Map<String, String> baseTm = service.getColumns(DATABASE, DIM_BASE_TRADEMARK, skuInfo.get("tm_id"), "info.tm_name");
                    dim = new JSONObject();
                    dim.put("sku_name", skuInfo.get("sku_name"));
                    dim.put("spu_id", skuInfo.get("spu_id"));
                    dim.put("spu_name", spuInfo.get("spu_name"));
                    dim.put("tm_id", skuInfo.get("tm_id"));
                    dim.put("tm_name", baseTm.get("tm_name"));
                    dim.put("category1_id", baseCat2.get("category1_id"));
                    dim.put("category1_name", baseCat1.get("name"));
                    dim.put("category2_id", baseCat3.get("category2_id"));
                    dim.put("category2_name", baseCat2.get("name"));
                    dim.put("category3_id", skuInfo.get("category3_id"));
                    dim.put("category3_name", baseCat3.get("name"));
                    client.setex(skuId, 60 * 60 * 24, dim.toString());
                }
                res.add(TradeSkuOrderBean.builder()
                        .startTime(row.getAs("startTime"))
                        .endTime(row.getAs("endTime"))
                        .skuId(skuId)
                        .totalOrderPrice(row.getAs("total_order_price"))
                        .totalSkuNum(row.getAs("total_sku_num"))
                        .skuName(dim.getString("sku_name"))
                        .spuId(dim.getString("spu_id"))
                        .spuName(dim.getString("spu_name"))
                        .trademarkId(dim.getString("tm_id"))
                        .trademarkName(dim.getString("tm_name"))
                        .category1Id(dim.getString("category1_id"))
                        .category1Name(dim.getString("category1_name"))
                        .category2Id(dim.getString("category2_id"))
                        .category2Name(dim.getString("category2_name"))
                        .category3Id(dim.getString("category3_id"))
                        .category3Name(dim.getString("category3_name"))
                        .build());
            }

            HBaseConnectionUtil.closeConnection(conn);
            RedisUtil.closeClient(client);

            return res.iterator();
        }, Encoders.bean(TradeSkuOrderBean.class));

        data.show();
    }
}

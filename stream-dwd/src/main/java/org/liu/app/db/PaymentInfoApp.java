package org.liu.app.db;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.liu.bean.TopicMeta;
import org.liu.common.app.AppBase;
import org.liu.common.service.HBaseService;
import org.liu.common.util.HBaseConnectionUtil;
import org.liu.common.util.StreamUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.*;
import static org.liu.common.constant.Constant.*;

public class PaymentInfoApp extends AppBase {
    public static void main(String[] args) {
        new PaymentInfoApp().start(args, 4);
    }

    @Override
    public void etl(SparkSession spark, String[] args) {
        Dataset<Row> source = kafkaStream(TOPIC_DB);

        // Data cleanse and transform
        Dataset<Row> paymentInfo = source.select(from_json(col("value"), new TopicMeta("PaymentInfo").getSchema()).as("columns"))
                .filter("columns.database = 'gmall' " +
                        "AND columns.table = 'payment_info' " +
                        "AND columns.type = 'update' " +
                        "AND columns.data.payment_status = '1602'")
                .select("columns.data.*").withColumnRenamed("callback_time", "payment_time").drop("payment_status");

        // Join order detail table and base_dic/user_info table to enrich columns
        // Business: which order contributes to payment? order_time <= payment_time <= order_time + 15 minutes
        Dataset<Row> baseDic = deltaTable(DIM_BASE_DIC).filter("parent_code = 11").select("dic_code", "dic_name");
        Dataset<Row> orderDetail = deltaTableStream(DWD_ORDER_DETAIL)
                .select("order_id", "create_time", "sku_id", "province_id", "activity_id", "activity_rule_id", "coupon_id", "sku_name", "order_price", "sku_num", "split_total_amount", "split_activity_amount", "split_coupon_amount")
                .withColumnRenamed("order_id", "id")
                .withWatermark("create_time", "1 minute");
        paymentInfo.withWatermark("payment_time", "1 minute");

        paymentInfo = paymentInfo.as("p")
                .join(
                        orderDetail.as("o"),
                        expr("p.order_id = o.id AND p.payment_time BETWEEN o.create_time AND o.create_time + INTERVAL 15 minutes"),
                        "left"
                ).select("p.*", "o.*").drop("id", "create_time");

        try {
            paymentInfo.writeStream()
                    .option("checkpointLocation", StreamUtil.getTableCheckpointPath(DWD_LAYER, DWD_PAYMENT_INFO))
                    .foreachBatch((src, id) -> {
                        src = src.as("p")
                                .join(baseDic.as("b"), expr("p.payment_type = b.dic_code"), "left")
                                .select(col("p.*"), col("b.dic_name").as("payment_name"))
                                .withColumn("user_name", lit(""))
                                .withColumn("date", date_format(col("payment_time"), "yyyy-MM-dd"));

                        // user_info is too large and changing quickly, so HBase is a good choice to randomly fetch record
                        // We can also add redis for caching to save resource and reduce latency
                        src = src.mapPartitions((MapPartitionsFunction<Row, Row>) partition -> {
                            Connection conn = HBaseConnectionUtil.newConnection();
                            HBaseService service = new HBaseService(conn);
                            ArrayList<Row> res = new ArrayList<>();
                            while (partition.hasNext()) {
                                Row row = partition.next();
                                String user_id = row.getAs("user_id");
                                String userName = service.getColumn(DATABASE, DIM_USER_INFO, user_id, "info", "name");
                                if (userName != null) {
                                    HashMap<String, Object> map = new HashMap<>();
                                    map.put("user_name", userName);
                                    res.add(StreamUtil.updateRow(row, map));
                                } else res.add(row);
                            }
                            HBaseConnectionUtil.closeConnection(conn);
                            return res.iterator();
                        }, RowEncoder.apply(src.schema()));

                        // Write to delta table
                        batchToDeltaTable(src, DWD_LAYER, DWD_PAYMENT_INFO, SaveMode.Append, "date", true);
                    }).start();
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        }
    }
}

package org.liu.app.db;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.liu.bean.TopicMeta;
import org.liu.common.app.AppBase;

import static org.apache.spark.sql.functions.*;
import static org.liu.common.constant.Constant.*;

public class OrderDetailApp extends AppBase {
    public static void main(String[] args) {
        new OrderDetailApp().start(args, 4);
    }

    @Override
    public void etl(SparkSession spark, String[] args) {
        // Ingest streaming source from kafka
        Dataset<Row> source = kafkaStream(TOPIC_DB);

        // Split out required table data and set watermarks to limit event delay(transmit time from source to spark engine)
        String watermark = "1 minute";
        Dataset<Row> orderDetail = source.select(from_json(col("value"), new TopicMeta("OrderDetail").getSchema()).as("columns"))
                .filter("columns.database = 'gmall' AND columns.table = 'order_detail' AND columns.type = 'insert'")
                .select("columns.data.*").withWatermark("create_time", watermark);
        Dataset<Row> orderInfo = source.select(from_json(col("value"), new TopicMeta("OrderInfo").getSchema()).as("columns"))
                .filter("columns.database = 'gmall' AND columns.table = 'order_info' AND columns.type = 'insert'")
                .select("columns.data.*").withWatermark("create_time", watermark);
        Dataset<Row> orderDetailActivity = source.select(from_json(col("value"), new TopicMeta("OrderDetailActivity").getSchema()).as("columns"))
                .filter("columns.database = 'gmall' AND columns.table = 'order_detail_activity' AND columns.type = 'insert'")
                .select("columns.data.*").withWatermark("create_time", watermark);
        Dataset<Row> orderDetailCoupon = source.select(from_json(col("value"), new TopicMeta("OrderDetailCoupon").getSchema()).as("columns"))
                .filter("columns.database = 'gmall' AND columns.table = 'order_detail_coupon' AND columns.type = 'insert'")
                .select("columns.data.*").withWatermark("create_time", watermark);

        /*
        constraint on event-time (sources correlation)
        source state will be dropped if no other sources can contribute to it because late data is dropped due to global watermark.
        Flow: late data is dropped, then engine will drop source states based on that.
        global watermark = min(source watermarks used for the stateful operation)
        - can ensure that no data is accidentally dropped as too late if one of the streams falls behind the others.
        - But may consume too much memory to store state, because global watermark will stop advancing!
        set spark.sql.streaming.multipleWatermarkPolicy = max
        - any data fall behind will be dropped. Fault tolerance is bad(e.g. source stopped by incident) but will strictly store limited state
        Consider a case: normally, transmit time + event time constraint is enough, but incident happens, one source emit lag behind or even stopped!
        How? sacrifice memory? how if the source stop too much time which may case OOM? set RocksDB limit the memory usage? Recovery will not be a big problem? if set multipleWatermarkPolicy = max, how if source frequently stops a little bit? too much data will be dropped!
        So case by case, but be judicious if set multipleWatermarkPolicy = max

        If one record is 1kb, watermark 10 minutes may consume several hundreds of MB for multiple stream join. Balanced by multiple workers.
         */
        orderDetail = orderDetail.as("a")
                .join(orderInfo.as("b"), expr("a.order_id = b.id AND " +
                        "a.create_time >= b.create_time - interval 10 seconds AND " +
                        "a.create_time <= b.create_time + interval 10 seconds"), "inner")
                .join(orderDetailActivity.as("c"), expr("a.order_id = c.order_id AND " +
                        "a.create_time >= c.create_time - interval 10 seconds AND " +
                        "a.create_time <= c.create_time + interval 10 seconds"), "left")
                .join(orderDetailCoupon.as("d"), expr("a.order_id = d.order_id AND " +
                        "a.create_time >= d.create_time - interval 10 seconds AND " +
                        "a.create_time <= d.create_time + interval 10 seconds"), "left")
                .select("a.*", "b.province_id", "c.activity_id", "c.activity_rule_id", "d.coupon_id")
                .withColumn("date", date_format(col("create_time"), "yyyy-MM-dd"));

        // write to delta table
        streamToDeltaTable(orderDetail, DWD_LAYER, DWD_ORDER_DETAIL, "date");
    }
}

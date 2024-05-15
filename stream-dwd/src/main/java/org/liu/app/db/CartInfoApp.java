package org.liu.app.db;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.liu.bean.TopicMeta;
import org.liu.common.app.AppBase;

import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_json;
import static org.liu.common.constant.Constant.*;

public class CartInfoApp extends AppBase {
    public static void main(String[] args) {
        new CartInfoApp().start(args, 4);
    }

    @Override
    public void etl(SparkSession spark, String[] args) {
        // Ingest streaming source from kafka
        Dataset<Row> source = kafkaStream(TOPIC_DB);

        // Data cleanse and transform
        source.select(from_json(col("value"), new TopicMeta("CartInfo").getSchema()).as("columns"))
                .filter("(columns.database = 'gmall' " +
                        "AND columns.table = 'cart_info' " +
                        "AND columns.type = 'insert') " +
                        "OR " +
                        "(columns.type = 'update' " +
                        "AND isnotnull(columns.old) " +
                        "AND CAST(columns.data.sku_num AS bigint) > CAST(columns.old['sku_num'] AS bigint))")
                .select("columns.*")
                .createOrReplaceTempView("source");
        source = spark.sql("SELECT\n" +
                "    data.id,\n" +
                "    data.user_id,\n" +
                "    data.sku_id,\n" +
                "    if(type != 'update', data.cart_price, if(isnull(old['cart_price']), 0, cast(data.cart_price as bigint) - cast(old['cart_price'] as bigint))) cart_price,\n" +
                "    if(type != 'update', data.sku_num, cast(data.sku_num as bigint) - cast(old['sku_num'] as bigint)) sku_num,\n" +
                "    data.sku_name,\n" +
                "    data.is_checked,\n" +
                "    data.create_time,\n" +
                "    data.operate_time,\n" +
                "    data.is_ordered,\n" +
                "    data.order_time,\n" +
                "    date_format(data.create_time, 'yyyy-MM-dd') date\n" +
                "FROM source");

        // Write to delta table
        streamToDeltaTable(source, DWD_LAYER, DWD_CART_INFO, "date");
    }
}

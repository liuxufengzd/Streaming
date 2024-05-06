package org.liu.app.db;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.liu.bean.TopicMeta;
import org.liu.common.app.AppBase;

import static org.apache.spark.sql.functions.*;
import static org.liu.common.constant.Constant.*;

public class UserInfoApp extends AppBase {
    public static void main(String[] args) {
        new UserInfoApp().start(args, 4);
    }

    @Override
    public void etl(SparkSession spark, String[] args) {
        Dataset<Row> source = kafkaStream(spark, TOPIC_DB);

        source = source
                .select(from_json(col("value"), new TopicMeta("UserInfo").getSchema()).as("columns"))
                .filter("columns.database = 'gmall' AND columns.table = 'user_info' AND columns.type IN('insert', 'bootstrap-insert')")
                .select("columns.data.*")
                .withColumn("create_time", to_timestamp(col("create_time")))
                .withColumn("date", date_format(col("create_time"), "yyyy-MM-dd"));

        streamToDeltaTable(source, DWD_LAYER, DWD_USER_INFO, "date");
    }
}

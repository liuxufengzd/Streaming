package org.liu.app;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.liu.common.app.AppBase;

import static org.apache.spark.sql.functions.*;
import static org.liu.common.constant.Constant.*;

public class UserRegisterApp extends AppBase {
    public static void main(String[] args) {
        new UserRegisterApp().start(args, 4);
    }

    @Override
    public void etl(SparkSession spark, String[] args) {
        Dataset<Row> source = deltaTableStream(spark, DWD_USER_INFO)
                .select("id", "create_time");

        source = source.withWatermark("create_time", "3 seconds")
                .groupBy(window(col("create_time"), "10 seconds", "5 seconds"))
                .agg(count("id").as("registerCt"))
                .withColumn("date", date_format(col("window.end"), "yyyy-MM-dd"));

        streamToDeltaTable(source, DWS_LAYER, DWS_USER_REGISTER, "date");
    }
}

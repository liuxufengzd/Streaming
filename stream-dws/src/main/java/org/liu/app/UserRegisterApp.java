package org.liu.app;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.liu.common.app.AppBase;

import static org.apache.spark.sql.functions.*;
import static org.liu.common.constant.Constant.*;

public class UserRegisterApp extends AppBase {
    public static void main(String[] args) {
        new UserRegisterApp().start(args, 4);
    }

    @Override
    public void etl(SparkSession spark, String[] args) {
        Dataset<Row> source = deltaTableStream(DWD_USER_INFO)
                .select("id", "create_time");

        source = source.withWatermark("create_time", "3 seconds")
                .groupBy(window(col("create_time"), "10 seconds", "5 seconds"))
                .agg(count("id").as("registerCt"))
                .withColumn("startTime", col("window.start").cast(DataTypes.StringType))
                .withColumn("endTime", col("window.end").cast(DataTypes.StringType))
                .withColumn("date", to_date(col("startTime"), "yyyy-MM-dd HH:mm:ss"))
                .drop("window");

        /*
        create table if not exists gmall.dws_user_register
        (
            `startTime`   DATETIME,
            `endTime`     DATETIME,
            `date`        DATE NOT NULL,
            `registerCt` BIGINT REPLACE
        )
        engine = olap
        aggregate key (`startTime`,`endTime`,`date`)
        partition by LIST(`date`)(
        PARTITION `p20220604`  VALUES IN ("2022-06-03"),
         PARTITION `p20220604`  VALUES IN ("2022-06-04"),
         PARTITION `p20220605`  VALUES IN ("2022-06-05"),
         PARTITION `p20220606`  VALUES IN ("2022-06-06"),
         PARTITION `p20220607`  VALUES IN ("2022-06-07"),
        PARTITION `p20220608`  VALUES IN ("2022-06-08"),
        PARTITION `p20220609`  VALUES IN ("2022-06-09"),
        PARTITION `p20220610`  VALUES IN ("2022-06-10"),
        PARTITION `p20220618`  VALUES IN ("2022-06-18")
)
        distributed by hash(`startTime`) buckets 10
        properties (
        "replication_num" = "1"
        );
         */
        streamToDorisTable(source, DWS_LAYER, DWS_USER_REGISTER);
    }
}

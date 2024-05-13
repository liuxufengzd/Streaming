package org.liu.app;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.liu.common.app.AppBase;
import org.liu.udf.SentenceSplit;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.liu.common.constant.Constant.*;

public class SearchKeywordApp extends AppBase {
    public static void main(String[] args) {
        new SearchKeywordApp().start(args, 4);
    }

    @Override
    public void etl(SparkSession spark, String[] args) {
        // Ingest streaming source from delta table
        Dataset<Row> source = deltaTableStream(TOPIC_LOG_PAGE);

        // Data cleanse and transform
        source = source
                .filter("last_page_id = 'search' AND item_type = 'keyword' AND isnotnull(item)")
                .select("item", "ts");

        spark.udf().register("splitKeywords", new SentenceSplit(), StringType);
        source.createOrReplaceTempView("source");
        source = spark.sql("SELECT keyword, ts FROM\n" +
                "(SELECT splitKeywords(item) keywords, ts FROM source)\n" +
                "LATERAL VIEW EXPLODE(split(keywords, ',')) tmp AS keyword");

        // Assume the transmit latency is 30 seconds. We need 10 seconds window to track aggregation metrics
        source = source.withWatermark("ts", "30 seconds")
                .groupBy(
                        window(col("ts"), "10 seconds"),
                        col("keyword")
                ).count()
                .withColumn("startTime", col("window.start").cast(DataTypes.StringType))
                .withColumn("endTime", col("window.end").cast(DataTypes.StringType))
                .withColumn("date", to_date(col("startTime"), "yyyy-MM-dd HH:mm:ss"))
                .drop("window");

        // Write to doris table
        /*
        create table if not exists gmall.dws_search_keyword_count
        (
            `startTime`   DATETIME,
            `endTime`     DATETIME,
            `date`        DATE NOT NULL,
            `keyword`  VARCHAR(100),
            `count` BIGINT REPLACE
        )
        engine = olap
        aggregate key (`startTime`,`endTime`,`date`,`keyword`)
        partition by LIST(`date`)(
        PARTITION `p20220608`  VALUES IN ("2022-06-08"),
        PARTITION `p20220609`  VALUES IN ("2022-06-09"),
        PARTITION `p20220610`  VALUES IN ("2022-06-10"),
        PARTITION `p20220618`  VALUES IN ("2022-06-18")
        )
        distributed by hash(`keyword`) buckets 10
        properties (
        "replication_num" = "1"
        );
         */
        streamToDorisTable(source, DWS_LAYER, DWS_SEARCH_KEYWORD_COUNT);
    }
}

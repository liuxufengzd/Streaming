package org.liu.app;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
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
        Dataset<Row> source = deltaTableStream(spark, TOPIC_LOG_PAGE);

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
                .withColumn("date", date_format(col("window.end"), "yyyy-MM-dd"));

        // Write to delta table
        streamToDeltaTable(source, DWS_LAYER, DWS_SEARCH_KEYWORD_COUNT, "date");
    }
}

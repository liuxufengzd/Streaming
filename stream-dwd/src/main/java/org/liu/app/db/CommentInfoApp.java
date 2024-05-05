package org.liu.app.db;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.liu.bean.TopicMeta;
import org.liu.common.app.AppBase;

import static org.apache.spark.sql.functions.*;
import static org.liu.common.constant.Constant.*;

public class CommentInfoApp extends AppBase {
    public static void main(String[] args) {
        new CommentInfoApp().start(args, 4);
    }

    @Override
    public void etl(SparkSession spark, String[] args) {
        // Ingest source data
        Dataset<Row> source = kafkaStream(spark, TOPIC_DB);

        // Filter out unneeded data
        source = source.select(from_json(col("value"), new TopicMeta("CommentInfo").getSchema()).as("columns"))
                .where("columns.database = 'gmall' AND columns.table = 'comment_info' AND isnotnull(columns.data) AND columns.type = 'insert'")
                .select("columns.data.*");

        // Enrich source with dimensional table. Small static table can be loaded for each micro batch
        Dataset<Row> baseDic = deltaTable(spark, DIM_BASE_DIC)
                .filter("parent_code = 12")
                .select("dic_code", "dic_name");
        baseDic.persist();
        source = source.join(baseDic, source.col("appraise").equalTo(baseDic.col("dic_code")), "left")
                .withColumn("appraise", baseDic.col("dic_name"))
                .withColumn("date", date_format(col("create_time"), "yyyy-MM-dd"))
                .drop("dic_code", "dic_name");

        // Write to delta table
        streamToDeltaTable(source, DWD_LAYER, DWD_COMMENT_INFO, "date");
        baseDic.unpersist();
    }
}

package org.liu.app;

import io.delta.tables.DeltaTable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.liu.common.app.AppBase;
import org.liu.common.util.StreamUtil;

import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.*;
import static org.liu.common.constant.Constant.*;

public class DimProcessApp extends AppBase {
    public static void main(String[] args) {
        new DimProcessApp().start(args, 4);
    }

    @Override
    public void etl(SparkSession spark, String[] args) {
        // Actually we can make maxwell monitor table groups for different topics to avoid trigger running too frequently
        Dataset<Row> source = kafkaStream(spark, TOPIC_DB);

        // Define raw log schema
        var dataSchema = new StructType()
                .add(DIM_PROCESS_ID, StringType)
                .add(DIM_PROCESS_SOURCE_TABLE, StringType)
                .add(DIM_PROCESS_SINK_TABLE, StringType)
                .add(DIM_PROCESS_ROW_KEY, StringType)
                .add(DIM_PROCESS_SCHEMA, StringType)
                .add(DIM_PROCESS_PARTITION_BY, StringType)
                .add(DIM_PROCESS_COLUMN_FAMILY, StringType)
                .add(DIM_PROCESS_TO_HBASE, IntegerType);

        var schema = new StructType()
                .add("database", StringType)
                .add("table", StringType)
                .add("type", StringType)
                .add("data", dataSchema)
                .add("ts", LongType);
        try {
            // Normalize raw stream data, clean up data
            source = source
                    .select(from_json(col("value"), schema).as("value"))
                    .na().drop()
                    .where(String.format("value.type != 'bootstrap-start' " +
                                    "AND value.type != 'bootstrap-complete' " +
                                    "AND value.database = '%s' " +
                                    "AND value.table = '%s'",
                            DATABASE, DIM_PROCESS_TABLE))
                    .select(
                            col("value.data.*"),
                            col("value.type").as("type"),
                            from_unixtime(col("value.ts"), "yyyy-MM-dd HH:mm:ss").as("time")
                    ).na().drop(new String[]{
                            DIM_PROCESS_ID,
                            DIM_PROCESS_SOURCE_TABLE,
                            DIM_PROCESS_SINK_TABLE,
                            DIM_PROCESS_ROW_KEY,
                            DIM_PROCESS_SCHEMA,
                            DIM_PROCESS_TO_HBASE,
                            "type",
                            "time"});

            // Write to delta table
//            spark.sql("CREATE DATABASE IF NOT EXISTS " + DELTA_DB);
            DeltaTable table = DeltaTable
                    .createIfNotExists(spark)
                    .addColumns(dataSchema)
                    .addColumn("time", TimestampType)
                    .location(StreamUtil.getTablePath(DIM_LAYER, DIM_PROCESS_TABLE))
                    .tableName(DELTA_DB + "." + DIM_PROCESS_TABLE)
                    .execute();

            source.writeStream()
                    .option("checkpointLocation", StreamUtil.getTableCheckpointPath(DIM_LAYER, DIM_PROCESS_TABLE))
                    .option("mergeSchema", true)
                    .foreachBatch((src, batchId) -> {
                        if (!src.isEmpty()) {
                            table.as("target").merge(src.as("source"), "target.id = source.id")
                                    .whenMatched("source.type = 'update'").updateAll()
                                    .whenMatched("source.type = 'delete'").delete()
                                    .whenNotMatched().insertAll()
                                    .execute();
                        }
                    })
                    .start();
        } catch (TimeoutException ignore) {
        }
    }
}

package org.liu.common.app;

import io.delta.tables.DeltaTable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.liu.common.constant.Constant;
import org.liu.common.util.StreamUtil;

import java.io.Serializable;
import java.util.concurrent.TimeoutException;

import static org.liu.common.constant.Constant.DELTA_DB;

public abstract class AppBase implements Serializable {
    public abstract void etl(SparkSession spark, String[] args);

    public void start(String[] args, int parallelism) {
        SparkSession spark = SparkSession.builder()
                .appName("test")
                .master("yarn")
                .config("spark.sql.shuffle.partitions", parallelism)
                .config("spark.sql.sources.partitionOverwriteMode", Constant.PARTITION_OVERWRITE_MODE)
                .config("spark.sql.warehouse.dir", Constant.WAREHOUSE_DIR)
                .config("hive.metastore.uris", Constant.METASTORE_URI)
                .config("spark.sql.streaming.stateStore.providerClass", Constant.ROCKSDB_STATE_STORE)
                .config("spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled", true)
                .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", true)
                .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", false)
                .config("spark.sql.adaptive.enabled", true)
                .config("spark.sql.extensions", Constant.SPARK_EXTENSIONS)
                .config("spark.sql.catalog.spark_catalog", Constant.SPARK_CATALOG)
                .enableHiveSupport()
                .getOrCreate();

        etl(spark, args);

        try {
            spark.streams().awaitAnyTermination();
        } catch (StreamingQueryException e) {
            throw new RuntimeException(e);
        }
    }

    public Dataset<Row> kafkaStream(SparkSession spark, String topic) {
        return spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", Constant.KAFKA_BOOTSTRAP_SERVERS)
                .option("subscribe", topic)
                .option("failOnDataLoss", "false")
                .load()
                .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");
    }

    public Dataset<Row> deltaTableStream(SparkSession spark, String tableName) {
        return spark.readStream()
                .format("delta")
                .option("skipChangeCommits", "true") // Ignore any delete and update operations
                .option("maxFilesPerTrigger", 5)
                .option("maxBytesPerTrigger", "10MB")
                .table(getDeltaTableFullName(tableName));
    }

    public Dataset<Row> deltaTable(SparkSession spark, String tableName) {
        return DeltaTable.forName(spark, getDeltaTableFullName(tableName)).toDF();
    }

    // Refresh static table with a constant frequency, which is suitable for slowly changed dataset
    public Dataset<Row> deltaTable(SparkSession spark, String tableName, long refreshIntervalInMin) {
        Dataset<Row> table = deltaTable(spark, tableName);
        table.persist();
        try {
            spark.readStream()
                    .format("rate")
                    .option("rowsPerSecond", 1)
                    .option("numPartitions", 1)
                    .load()
                    .writeStream()
                    .foreachBatch((e, id) -> {
                        refreshTable(spark, table, tableName);
                    })
                    .queryName("Refresh-" + tableName)
                    .trigger(Trigger.ProcessingTime(refreshIntervalInMin * 60 * 1000))
                    .start();
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        }
        return table;
    }

    private void refreshTable(SparkSession spark, Dataset<Row> table, String tableName) {
        table.unpersist();
        table = deltaTable(spark, tableName);
        table.persist();
    }

    public void streamToDeltaTable(Dataset<Row> stream, String layer, String tableName, String dateColumn) {
        try {
            stream.writeStream().format("delta")
                    .partitionBy(dateColumn)
                    .option("checkpointLocation", StreamUtil.getTableCheckpointPath(layer, tableName))
                    .option("path", StreamUtil.getTablePath(layer, tableName))
                    .toTable(getDeltaTableFullName(tableName));
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    public void batchToDeltaTable(Dataset<Row> batch, String layer, String tableName, SaveMode mode, String dateColumn, boolean enableSchemaEvolution) {
        if (batch.isEmpty()) return;
        batch.write().format("delta")
                .mode(mode)
                .partitionBy(dateColumn)
                .option("mergeSchema", enableSchemaEvolution)
                .option("path", StreamUtil.getTablePath(layer, tableName))
                .saveAsTable(getDeltaTableFullName(tableName));
    }

    public void batchToDeltaTable(Dataset<Row> batch, String layer, String tableName, SaveMode mode, String dateColumn) {
        batchToDeltaTable(batch, layer, tableName, mode, dateColumn, false);
    }

    public void deltaTableConsole(SparkSession spark, String tableName) {
        try {
            spark.readStream()
                    .format("delta")
                    .table(getDeltaTableFullName(tableName))
                    .writeStream()
                    .format("console")
                    .start();
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    private String getDeltaTableFullName(String tableName) {
        return DELTA_DB + "." + tableName;
    }
}

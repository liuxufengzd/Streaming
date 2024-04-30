package org.liu.common.app;

import io.delta.tables.DeltaTable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
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
                .config("spark.sql.adaptive.enabled", true)
                .config("spark.sql.extensions", Constant.SPARK_EXTENSIONS)
                .config("spark.sql.catalog.spark_catalog", Constant.SPARK_CATALOG)
                .enableHiveSupport()
                .getOrCreate();

        etl(spark, args);
    }

    public Dataset<Row> kafkaStream(SparkSession spark, String topic) {
        return spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", Constant.KAFKA_BOOTSTRAP_SERVERS)
                .option("subscribe", topic)
                .load()
                .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");
    }

    public Dataset<Row> deltaTable(SparkSession spark, String tableName) {
        return DeltaTable.forName(spark, tableName).toDF();
    }

    public void streamToDeltaTable(Dataset<Row> stream, String layer, String tableName, OutputMode mode, String dateColumn) {
        try {
            stream.writeStream().format("delta")
                    .outputMode(mode)
                    .partitionBy(dateColumn)
                    .option("checkpointLocation", StreamUtil.getTableCheckpointPath(layer, tableName))
                    .option("path", StreamUtil.getTablePath(layer, tableName))
                    .toTable(DELTA_DB + "." + tableName);
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    public void batchToDeltaTable(Dataset<Row> batch, String layer, String tableName, SaveMode mode, String dateColumn) {
        if (batch.isEmpty()) return;
        batch.write().format("delta")
                .mode(mode)
                .partitionBy(dateColumn)
                .option("path", StreamUtil.getTablePath(layer, tableName))
                .saveAsTable(DELTA_DB + "." + tableName);
    }
}

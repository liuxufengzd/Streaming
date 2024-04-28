package org.liu.common.app;

import io.delta.tables.DeltaTable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.liu.common.constant.Constant;

import java.io.Serializable;

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

    public Dataset<Row> deltaBatch(SparkSession spark, String tableName) {
        return DeltaTable.forName(spark, tableName).toDF();
    }
}

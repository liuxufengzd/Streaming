package org.liu.common.constant;

public class Constant {
    public static final String WAREHOUSE_DIR = "hdfs://hadoop102:8020/user/lakehouse";
    public static final String METASTORE_URI = "thrift://hadoop102:9083";
    public static final String PARTITION_OVERWRITE_MODE = "dynamic";
    public static final String SPARK_EXTENSIONS = "io.delta.sql.DeltaSparkSessionExtension";
    public static final String SPARK_CATALOG = "org.apache.spark.sql.delta.catalog.DeltaCatalog";
    public static final String KAFKA_BOOTSTRAP_SERVERS = "hadoop102:9092,hadoop103:9092";
    public static final String DATABASE = "gmall";
    public static final String DIM_PROCESS_TABLE = "dim_process";
    public static final String TOPIC_DB = "topic_db";
    public static final String DELTA_DB = "stream";
    public static final String DIM_LAYER = "dim";

    // dim_process column names
    public static final String DIM_PROCESS_ID = "id";
    public static final String DIM_PROCESS_SOURCE_TABLE = "source_table";
    public static final String DIM_PROCESS_SINK_TABLE = "sink_table";
    public static final String DIM_PROCESS_ROW_KEY = "row_key";
    public static final String DIM_PROCESS_SCHEMA = "schema";
    public static final String DIM_PROCESS_COLUMN_FAMILY = "column_family";
    public static final String DIM_PROCESS_TO_HBASE = "to_hbase";
}

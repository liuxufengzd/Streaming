package org.liu.common.constant;

public class Constant {
    public static final String WAREHOUSE_DIR = "hdfs://hadoop102:8020/user/lakehouse";
    public static final String METASTORE_URI = "thrift://hadoop102:9083";
    public static final String DORIS_ENDPOINT = "hadoop104:7030";
    public static final String DORIS_USERNAME = "root";
    public static final String DORIS_PWD = "000000";
    public static final String PARTITION_OVERWRITE_MODE = "dynamic";
    public static final String ROCKSDB_STATE_STORE = "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider";
    public static final String SPARK_EXTENSIONS = "io.delta.sql.DeltaSparkSessionExtension";
    public static final String SPARK_CATALOG = "org.apache.spark.sql.delta.catalog.DeltaCatalog";
    public static final String KAFKA_BOOTSTRAP_SERVERS = "hadoop102:9092,hadoop103:9092";
    public static final String DATABASE = "gmall";
    public static final String DIM_PROCESS_TABLE = "dim_process";
    public static final String TOPIC_DB = "topic_db";
    public static final String TOPIC_LOG = "topic_log";
    public static final String DELTA_DB = "stream";
    public static final String DIM_LAYER = "dim";
    public static final String DWD_LAYER = "dwd";
    public static final String DWS_LAYER = "dws";

    // dim_process column names
    public static final String DIM_PROCESS_ID = "id";
    public static final String DIM_PROCESS_SOURCE_TABLE = "source_table";
    public static final String DIM_PROCESS_SINK_TABLE = "sink_table";
    public static final String DIM_PROCESS_ROW_KEY = "row_key";
    public static final String DIM_PROCESS_PARTITION_BY = "partitionBy";
    public static final String DIM_PROCESS_SCHEMA = "schema";
    public static final String DIM_PROCESS_COLUMN_FAMILY = "column_family";
    public static final String DIM_PROCESS_TO_HBASE = "to_hbase";

    // table names
    public static final String TOPIC_LOG_START = "topic_log_start";
    public static final String TOPIC_LOG_ERR = "topic_log_err";
    public static final String TOPIC_LOG_PAGE = "topic_log_page";
    public static final String TOPIC_LOG_DISPLAY = "topic_log_display";
    public static final String TOPIC_LOG_ACTION = "topic_log_action";
    public static final String DIM_BASE_DIC = "dim_base_dic";
    public static final String DIM_USER_INFO = "dim_user_info";
    public static final String DIM_SKU_INFO = "dim_sku_info";
    public static final String DIM_SPU_INFO = "dim_spu_info";
    public static final String DIM_BASE_CATEGORY1 = "dim_base_category1";
    public static final String DIM_BASE_CATEGORY2 = "dim_base_category2";
    public static final String DIM_BASE_CATEGORY3 = "dim_base_category3";
    public static final String DIM_BASE_TRADEMARK = "dim_base_trademark";
    public static final String DIM_BASE_PROVINCE = "dim_base_province";
    public static final String DWD_COMMENT_INFO = "dwd_comment_info";
    public static final String DWD_CART_INFO = "dwd_cart_info";
    public static final String DWD_USER_INFO = "dwd_user_info";
    public static final String DWD_ORDER_DETAIL = "dwd_order_detail";
    public static final String DWD_PAYMENT_INFO = "dwd_payment_info";
    public static final String DWS_SEARCH_KEYWORD_COUNT = "dws_search_keyword_count";
    public static final String DWS_TRAFFIC_PAGE_VIEW_COUNT = "dws_traffic_page_view_count";
    public static final String DWS_TRAFFIC_HOME_DETAILPAGE_VIEW_COUNT = "dws_traffic_home_detailPage_view_count";
    public static final String DWS_USER_LOGIN = "dws_user_login";
    public static final String DWS_USER_REGISTER = "dws_user_register";
    public static final String DWS_TRADE_CART_ADD = "dws_trade_cart_add";
    public static final String DWS_TRADE_PAYMENT_SUC = "dws_trade_payment_suc";
    public static final String DWS_TRADE_SKU_ORDER = "dws_trade_sku_order";
    public static final String DWS_TRADE_PROVINCE_ORDER = "dws_trade_province_order";
}

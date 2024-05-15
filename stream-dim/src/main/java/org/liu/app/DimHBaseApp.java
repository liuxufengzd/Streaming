package org.liu.app;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.liu.accumulator.DimProcessAccumulator;
import org.liu.common.app.AppBase;
import org.liu.common.bean.DimTableMeta;
import org.liu.common.service.HBaseService;
import org.liu.common.util.HBaseConnectionUtil;
import org.liu.common.util.RedisUtil;
import org.liu.common.util.StreamUtil;
import redis.clients.jedis.Jedis;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_json;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.liu.common.constant.Constant.*;

public class DimHBaseApp extends AppBase {
    public static void main(String[] args) {
        new DimHBaseApp().start(args, 4);
    }

    @Override
    public void etl(SparkSession spark, String[] args) {
        Dataset<Row> source = kafkaStream(TOPIC_DB);
        try {
            source.writeStream()
                    .option("checkpointLocation", StreamUtil.getTableCheckpointPath(DIM_LAYER, TOPIC_DB + "_hbase"))
                    .foreachBatch((src, id) -> {
                        process(spark, src);
                    }).start();
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    private void process(SparkSession spark, Dataset<Row> src) {
        Dataset<Row> dimProcess = deltaTable(DIM_PROCESS_TABLE)
                .filter(col(DIM_PROCESS_TO_HBASE).notEqual(0));

        // Parse and filter dimensional table source
        var sourceSchema = new StructType()
                .add("table", StringType, false)
                .add("type", StringType, false)
                .add("data", StringType, false);
        src = src
                .select(from_json(col("value"), sourceSchema).as("parsed"))
                .select("parsed.*")
                .filter("type != 'bootstrap-start' AND type != 'bootstrap-complete'")
                .join(dimProcess, col("table").equalTo(col(DIM_PROCESS_SOURCE_TABLE)), "left_semi")
                .select(col("data"), col("table"), col("type").as("_type_"));

        src.cache();

        DimProcessAccumulator metaAcc = new DimProcessAccumulator();
        spark.sparkContext().register(metaAcc);
        dimProcess.foreach(row -> {
            metaAcc.add(new AbstractMap.SimpleEntry<>(row.getAs(DIM_PROCESS_SOURCE_TABLE), StreamUtil.getDimMetaFromDimProcessRow(row)));
        });

        // Transform and write to hbase tables
        Map<String, DimTableMeta> metaMap = metaAcc.value();
        for (String tableName : metaMap.keySet()) {
            DimTableMeta meta = metaMap.get(tableName);
            Dataset<Row> df = src
                    .filter(col("table").equalTo(tableName))
                    .select(
                            from_json(col("data"), meta.schema).as("data"),
                            col("_type_")
                    )
                    .select("data.*", "_type_");

            if (!df.isEmpty()) {
                writeToHBase(df, meta);
            }
        }

        src.unpersist();
    }

    /*
    We can consider leveraging HBase for case:
    Record level reading/writing is required and table is large, which is costly and slow to load the whole static table even compared with HBase heavy metadata (e.g. user table).
    Create connection with HBase for each worker is not recommended because it will limit the scalability
    So use HBase with caution
     */
    private void writeToHBase(Dataset<Row> df, DimTableMeta meta) {
        df.foreachPartition(partition -> {
            ArrayList<Put> puts = new ArrayList<>();
            ArrayList<Delete> deletes = new ArrayList<>();
            Jedis client = RedisUtil.getClient();
            while (partition.hasNext()) {
                Row row = partition.next();
                var type = (String) row.getAs("_type_");
                var rowKey = (String) row.getAs(meta.rowKey);
                String rKey = RedisUtil.getKey(meta.sinkTable, rowKey);
                // We can also get old from CDC and update specific columns, which could be more efficient if updated column number is small
                if ("delete".equals(type)) {
                    deletes.add(new Delete(Bytes.toBytes(rowKey)));
                    client.del(rKey);
                } else {
                    if ("update".equals(type)) {
                        deletes.add(new Delete(Bytes.toBytes(rowKey)));
                        client.del(rKey);
                    }
                    String[] columnNames = row.schema().names();
                    for (String name : columnNames) {
                        if ("_type_".equals(name)) continue;
                        Put put = new Put(Bytes.toBytes(rowKey));
                        Object value = row.getAs(name);
                        if (value != null){
                            put.addColumn(Bytes.toBytes(meta.columnFamily), Bytes.toBytes(name), Bytes.toBytes(value.toString()));
                            puts.add(put);
                        }
                    }
                }
            }
            // Cannot use foreach, because creating connection for each row is drastically expensive
            // Each new connection for each micro batch (even somehow expensive)
            Connection conn = HBaseConnectionUtil.newConnection();
            HBaseService service = new HBaseService(conn);
            // service.createDatabase(DATABASE);
            service.createTableIfNotExist(DATABASE, meta.sinkTable, meta.columnFamily);
            if (!deletes.isEmpty()) {
                service.deleteRows(DATABASE, meta.sinkTable, deletes);
            }
            if (!puts.isEmpty()) {
                service.insertRows(DATABASE, meta.sinkTable, puts);
            }

            HBaseConnectionUtil.closeConnection(conn);
        });
    }
}

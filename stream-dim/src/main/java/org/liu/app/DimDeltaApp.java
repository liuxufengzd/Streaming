package org.liu.app;

import io.delta.tables.DeltaTable;
import io.delta.tables.DeltaTableBuilder;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.liu.accumulator.DimProcessAccumulator;
import org.liu.common.app.AppBase;
import org.liu.common.bean.dim.DimTableMeta;
import org.liu.common.util.StreamUtil;

import java.util.AbstractMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_json;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.liu.common.constant.Constant.*;

public class DimDeltaApp extends AppBase {
    public static void main(String[] args) {
        new DimDeltaApp().start(args, 4);
    }

    @Override
    public void etl(SparkSession spark, String[] args) {
        Dataset<Row> source = kafkaStream(TOPIC_DB);
        try {
            source.writeStream()
                    .option("checkpointLocation", StreamUtil.getTableCheckpointPath(DIM_LAYER, TOPIC_DB + "_delta"))
                    .foreachBatch((src, id) -> {
                        process(spark, src);
                    }).start();
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    private void process(SparkSession spark, Dataset<Row> src) {
        // Fetch whole dim_process table to ensure each micro batch can reference latest table
        // We can also control the frequency of fetching if dim_process is slowly changed and lenient requirement to save resource
        Dataset<Row> dimProcess = deltaTable(DIM_PROCESS_TABLE)
                .filter(col(DIM_PROCESS_TO_HBASE).equalTo(0));

        // Parse and filter dimensional table source
        // AQE can optimize join in foreachBatch
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

        src.persist();

        // Collect metadata of all dimensional tables
        /*
        In Spark Structured Streaming, static DataFrame operations are only executed once and not for each micro-batch.
        If you’re reading a static table outside the foreachBatch method, it will be read only once when the streaming query starts,
        and the same data will be used for each micro-batch. This is because the reading operation is not part of the streaming query plan that gets executed for each micro-batch.
        If you want to ingest a new table for each micro-batch, you should do it inside the foreachBatch method.
        This way, the ingestion operation becomes part of the processing for each micro-batch
         */
        DimProcessAccumulator metaAcc = new DimProcessAccumulator();
        spark.sparkContext().register(metaAcc);
        dimProcess.foreach(row -> {
            metaAcc.add(new AbstractMap.SimpleEntry<>(row.getAs(DIM_PROCESS_SOURCE_TABLE), StreamUtil.getDimMetaFromDimProcessRow(row)));
        });

        // Transform and write to delta tables
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
                writeToDelta(spark, df, meta);
            }
        }

        src.unpersist();
    }

    private void writeToDelta(SparkSession spark, Dataset<Row> df, DimTableMeta meta) {
        DeltaTableBuilder builder = DeltaTable.createIfNotExists(spark)
                .addColumns(meta.schema)
                .location(StreamUtil.getTablePath(DIM_LAYER, meta.sinkTable))
                .tableName(DELTA_DB + "." + meta.sinkTable);
        if (meta.partitionBy != null) {
            builder = builder.partitionedBy(meta.partitionBy);
        }
        // For normal write to keep idempotence, refer Idempotent table writes in foreachBatch: https://learn.microsoft.com/en-us/azure/databricks/structured-streaming/delta-lake
        // To keep idempotence, set condition for NotMatched (even though update may affect result for repeated micro-batch)
        builder.execute()
                .as("sink")
                .merge(df.as("source"), String.format("sink.%s = source.%s", meta.rowKey, meta.rowKey))
                .whenMatched("source._type_ = 'update'").updateAll()
                .whenMatched("source._type_ = 'delete'").delete()
                .whenNotMatched("source._type_ = 'insert' OR source._type_ = 'bootstrap-insert'").insertAll()
                .execute();
    }
}

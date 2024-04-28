package org.liu.app;

import io.delta.tables.DeltaTable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;
import org.liu.accumulator.DimProcessAccumulator;
import org.liu.common.bean.dim.DimTableMeta;
import org.liu.common.app.AppBase;
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
        Dataset<Row> source = kafkaStream(spark, TOPIC_DB);
        try {
            source.writeStream()
                    .option("checkpointLocation", StreamUtil.getTableCheckpointPath(DIM_LAYER, TOPIC_DB + "_delta"))
                    .foreachBatch((src, id) -> {
                        process(spark, src);
                    }).start().awaitTermination();
        } catch (StreamingQueryException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    private void process(SparkSession spark, Dataset<Row> src) {
        Dataset<Row> dimProcess = deltaBatch(spark, DELTA_DB + "." + DIM_PROCESS_TABLE)
                .filter(col(DIM_PROCESS_TO_HBASE).equalTo(0));

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
        DeltaTable.createIfNotExists(spark)
                .addColumns(meta.schema)
                .location(StreamUtil.getTablePath(DIM_LAYER, meta.sinkTable))
                .tableName(DELTA_DB + "." + meta.sinkTable)
                .execute()
                .as("sink")
                .merge(df.as("source"), String.format("sink.%s = source.%s", meta.rowKey, meta.rowKey))
                .whenMatched("source._type_ = 'update'").updateAll()
                .whenMatched("source._type_ = 'delete'").delete()
                .whenNotMatched().insertAll()
                .execute();
    }
}

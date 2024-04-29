package org.liu.app;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.streaming.GroupState;
import org.apache.spark.sql.streaming.GroupStateTimeout;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.TimestampType;
import org.liu.bean.LogTopicMeta;
import org.liu.common.accumulator.LogAccumulator;
import org.liu.common.app.AppBase;
import org.liu.common.util.DateUtil;
import org.liu.common.util.StreamUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Seq;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.liu.common.constant.Constant.*;

public class DwdLogApp extends AppBase {
    private static Logger logger = LoggerFactory.getLogger(DwdLogApp.class);

    public static void main(String[] args) {
        new DwdLogApp().start(args, 4);
    }

    @Override
    public void etl(SparkSession spark, String[] args) {
        // Import streaming data from kafka
        Dataset<Row> source = kafkaStream(spark, TOPIC_LOG);
        LogTopicMeta meta = new LogTopicMeta("TopicLog");
        StructType schema = meta.getStructType();

        // Parse data and filter out bad events
        Dataset<Row> parsedSource = source.select(from_json(col("value"), schema).as("columns"));
        var filterOut = "columns is null " +
                "OR columns.ts is null " +
                "OR columns.common is null " +
                "OR columns.common.mid is null";
        Dataset<Row> invalidData = parsedSource.where(filterOut);
        // if you’re using a sink that supports idempotent writes (like a Delta table),
        // then Spark’s checkpointing mechanism should be sufficient to ensure exactly-once semantics without needing to manually manage the batchId(in foreachBatch case)
//        String badTableName = TOPIC_DB + "_DwdLogApp_Bad";
//        invalidData.writeStream().outputMode("append").format("text")
//                        .option("checkpointLocation", StreamUtil.getTableCheckpointPath(DWD_LAYER, badTableName))
//                                .start(StreamUtil.getTablePath(DWD_LAYER, badTableName)).awaitTermination();

        Dataset<Row> validData = parsedSource.where(String.format("NOT (%s)", filterOut)).select(
                col("columns.*"),
                col("columns.common.mid").as("mid"),
                col("columns.common.is_new").as("is_new")
        ).withColumn("ts", from_unixtime(col("ts").divide(1000)).cast("timestamp"));


        // correct is_new for each specific device
        // mapGroupsWithState allows the function to return one and only one record for each input group1. This means that for every group of input data, you can generate a single output record.
        // On the other hand, flatMapGroupsWithState is more flexible and allows the function to return any number of records (including no records) for each input group1.
        // This means that for every group of input data, you can generate multiple output records or even no output record.
        validData = validData.withWatermark("ts", "5 seconds")
                .groupByKey((MapFunction<Row, String>) row -> (String) row.getAs("mid"), Encoders.STRING())
                .flatMapGroupsWithState(this::updateDeviceState, OutputMode.Update(), Encoders.STRING(), RowEncoder.apply(validData.schema()), GroupStateTimeout.NoTimeout());

        // Split stream and write to different delta tables
        try {
            validData.writeStream()
                    .option("checkpointLocation", StreamUtil.getTableCheckpointPath(DWD_LAYER, "testTable"))
                    .format("console")
                    .outputMode("update")
                    .start().awaitTermination();
        } catch (StreamingQueryException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    private Iterator<Row> updateDeviceState(String mid, Iterator<Row> rows, GroupState<String> state) {
        ArrayList<Row> res = new ArrayList<>();
        while (rows.hasNext()) {
            Row row = rows.next();
            String date = new SimpleDateFormat("yyyy-MM-dd").format(row.getAs("ts"));
            boolean isNew = row.getAs("is_new") != "0";
            if (isNew) {
                HashMap<String, Object> modifyMap = new HashMap<>();
                if (state.exists() && !state.get().equals(date)) {
                    modifyMap.put("is_new", "0");
                    res.add(StreamUtil.updateRow(row, modifyMap));
                } else if (!state.exists()) {
                    state.update(date);
                    if (row.getAs("is_new") != "1") {
                        modifyMap.put("is_new", "1");
                        res.add(StreamUtil.updateRow(row, modifyMap));
                    }
                } else {
                    if (row.getAs("is_new") != "1") {
                        modifyMap.put("is_new", "1");
                        res.add(StreamUtil.updateRow(row, modifyMap));
                    }
                }
            } else if (!state.exists()) {
                state.update(DateUtil.addNDaysToDate(date, -1));
            } else {
                // is not new
            }
        }
        return res.iterator();
    }
}

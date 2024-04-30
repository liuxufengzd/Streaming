package org.liu.app;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.streaming.GroupState;
import org.apache.spark.sql.streaming.GroupStateTimeout;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;
import org.liu.bean.DeviceState;
import org.liu.bean.LogTopicMeta;
import org.liu.common.app.AppBase;
import org.liu.common.util.DateUtil;
import org.liu.common.util.StreamUtil;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.*;
import static org.liu.common.constant.Constant.DWD_LAYER;
import static org.liu.common.constant.Constant.TOPIC_LOG;

public class DwdLogApp extends AppBase {

    public static void main(String[] args) {
        new DwdLogApp().start(args, 4);
    }

    @Override
    public void etl(SparkSession spark, String[] args) {
        // Import streaming data from kafka
        Dataset<Row> source = kafkaStream(spark, TOPIC_LOG);

        // Parse data and filter out bad events
        StructType schema = new LogTopicMeta("TopicLog").getStructType();
        Dataset<Row> parsedSource = source.select(from_json(col("value"), schema).as("columns"), col("value"));
        var filterOut = "isnull(columns) " +
                "OR isnull(columns.ts) " +
                "OR isnull(columns.common) " +
                "OR isnull(columns.common.mid)";

        // Write invalid data for monitoring
        Dataset<Row> invalidData = parsedSource.where(filterOut).select(col("value"),current_date().as("date"));
        streamToDeltaTable(invalidData, DWD_LAYER, "topic_log_invalid", OutputMode.Append(), "date");

        Dataset<Row> validData = parsedSource.where(String.format("NOT (%s)", filterOut)).select(
                col("columns.*"),
                col("columns.common.mid").as("mid"),
                col("columns.common.is_new").as("is_new")
        ).withColumn("ts", from_unixtime(col("ts").divide(1000)).cast("timestamp"));

        /*
        correct is_new for each specific device
        mapGroupsWithState allows the function to return one and only one record for each input group.
        flatMapGroupsWithState allows the function to return any number of records (including no records) for each input group.
        */
        validData = validData.groupByKey((MapFunction<Row, String>) row -> (String) row.getAs("mid"), Encoders.STRING())
                .flatMapGroupsWithState(this::updateDeviceState, OutputMode.Append(),
                        Encoders.bean(DeviceState.class), RowEncoder.apply(validData.schema()), GroupStateTimeout.NoTimeout());

        /*
        Split stream and write to different delta tables
        1. Using multiple Structured Streaming writers for best parallelization and throughput.
        2. Using foreachBatch to write to multiple sinks serializes the execution of streaming writes, which can increase latency for each micro-batch.
        Why? if one operator failed, spark can reprocess the micro batch to ensure at least once.
        If in parallel, resource has cost, some writings are successful, but have to rewrite again if any other writing failure. If in sequential, spark can stop processing subsequent operations once any error happens.
        This should be a trade-off between latency, cost and exactly-once guarantee.

        In Spark Structured Streaming, each writeStream action triggers a separate streaming query, and each query operates independently.
        They do not share state or computed DataFrames, even within the same micro-batch. So repeated computation is easy to happen (cache is not permitted in streaming).

        If you’re using a sink that supports idempotent writes (like a Delta table),
        then Spark’s checkpointing mechanism should be sufficient to ensure exactly-once semantics without needing to manually manage the batchId(in foreachBatch case)
         */
        try {
            validData.writeStream()
                    .option("checkpointLocation", StreamUtil.getTableCheckpointPath(DWD_LAYER, TOPIC_LOG))
                    .foreachBatch((src, id) -> {
                        src.cache();

                        Dataset<Row> startData = src.where("isnotnull(start)").select("common.*", "start.*", "ts")
                                .withColumn("date", date_format(col("ts"), "yyyy-MM-dd"));
                        batchToDeltaTable(startData, DWD_LAYER, "topic_log_start", SaveMode.Append, "date");

                        Dataset<Row> errorData = src.where("isnotnull(err)").select("err.*", "ts")
                                .withColumn("date", date_format(col("ts"), "yyyy-MM-dd"));
                        batchToDeltaTable(errorData, DWD_LAYER, "topic_log_err", SaveMode.Append, "date");

                        Dataset<Row> pageData = src.where("isnotnull(page)").select("common.*", "page.*", "ts")
                                .withColumn("date", date_format(col("ts"), "yyyy-MM-dd"));
                        batchToDeltaTable(pageData, DWD_LAYER, "topic_log_page", SaveMode.Append, "date");

                        Dataset<Row> displayData = src.where("isnotnull(displays)");
                        displayData.createOrReplaceGlobalTempView("displayData");
                        displayData = spark.sql("SELECT common.*, page.*, d.display_type, d.item display_item, d.item_type display_item_type, d.pos_seq, d.pos_id, ts " +
                                        "FROM global_temp.displayData LATERAL VIEW EXPLODE(displays) tmp as d")
                                .withColumn("date", date_format(col("ts"), "yyyy-MM-dd"));
                        batchToDeltaTable(displayData, DWD_LAYER, "topic_log_display", SaveMode.Append, "date");

                        Dataset<Row> actionData = src.where("isnotnull(actions)");
                        actionData.createOrReplaceGlobalTempView("actionData");
                        actionData = spark.sql("SELECT common.*, page.*, a.action_id, a.item action_item, a.item_type action_item_type, ts " +
                                        "FROM global_temp.actionData LATERAL VIEW EXPLODE(actions) tmp as a")
                                .withColumn("date", date_format(col("ts"), "yyyy-MM-dd"));
                        batchToDeltaTable(actionData, DWD_LAYER, "topic_log_action", SaveMode.Append, "date");

                        src.unpersist();
                    }).start();

            spark.streams().awaitAnyTermination();
        } catch (TimeoutException | StreamingQueryException e) {
            throw new RuntimeException(e);
        }
    }

    private Iterator<Row> updateDeviceState(String mid, Iterator<Row> rows, GroupState<DeviceState> state) {
        ArrayList<Row> res = new ArrayList<>();
        while (rows.hasNext()) {
            Row row = rows.next();
            String eventTime = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss").format(row.getAs("ts"));
            String eventDate = new SimpleDateFormat("yyyy-MM-dd").format(row.getAs("ts"));
            if (state.exists()) {
                // Drop the data that is late for this device. Watermark cannot use here because we cannot use ts(or window) for grouping
                long duration = DateUtil.getDurationInSeconds(eventTime, state.get().lastVisitTime);
                long TRACK_DEVICE_DATA_LATENCY = 5;
                if (duration > TRACK_DEVICE_DATA_LATENCY) continue;
                if (duration < 0) {
                    state.update(new DeviceState(state.get().firstVisitDate, eventTime));
                }
            }

            // Correct the is_new flag
            boolean isNew = row.getAs("is_new") != "0";
            if (isNew) {
                HashMap<String, Object> modifyMap = new HashMap<>();
                if (state.exists() && !state.get().firstVisitDate.equals(eventDate)) {
                    modifyMap.put("is_new", "0");
                    res.add(StreamUtil.updateRow(row, modifyMap));
                } else if (!state.exists()) {
                    state.update(new DeviceState(eventDate, eventTime));
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
                state.update(new DeviceState(DateUtil.addNDaysToDate(eventDate, -1), eventTime));
            }
        }
        return res.iterator();
    }
}

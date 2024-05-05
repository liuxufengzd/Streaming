package org.liu.app;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.streaming.GroupState;
import org.apache.spark.sql.streaming.GroupStateTimeout;
import org.apache.spark.sql.streaming.OutputMode;
import org.liu.common.app.AppBase;
import org.liu.common.util.DateUtil;
import org.liu.common.util.StreamUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import static org.apache.spark.sql.functions.*;
import static org.liu.common.constant.Constant.*;

public class TrafficVcChArIsNewPageViewApp extends AppBase {
    public static void main(String[] args) {
        new TrafficVcChArIsNewPageViewApp().start(args, 4);
    }

    @Override
    public void etl(SparkSession spark, String[] args) {
        // Ingest streaming source from delta table
        Dataset<Row> source = deltaTableStream(spark, TOPIC_LOG_PAGE)
                .select("mid", "last_page_id", "ar", "ch", "vc", "is_new", "ts", "during_time")
                .withColumn("uv", lit(0))
                .withColumn("sv", lit(0))
                .withColumn("pv", lit(1));

        // Data cleanse and transform
        source = source.groupByKey((MapFunction<Row, String>) r -> (String) r.getAs("mid"), Encoders.STRING())
                .flatMapGroupsWithState(this::stateHandler, OutputMode.Append(), Encoders.STRING(), RowEncoder.apply(source.schema()), GroupStateTimeout.ProcessingTimeTimeout())
                .drop("mid", "last_page_id");

        source = source
                .withWatermark("ts", "3 seconds")
                .groupBy(
                        col("ar"),
                        col("ch"),
                        col("vc"),
                        col("is_new"),
                        window(col("ts"), "10 seconds")
                ).agg(sum("pv").as("pvCt"),
                        sum("uv").as("uvCt"),
                        sum("sv").as("svCt"),
                        sum("during_time").as("durSum"))
                .withColumn("date", date_format(col("window.end"), "yyyy-MM-dd"));

        // Write to delta table
        streamToDeltaTable(source, DWS_LAYER, DWS_TRAFFIC_PAGE_VIEW_COUNT, "date");
    }

    private Iterator<Row> stateHandler(String mid, Iterator<Row> rows, GroupState<String> lastVisitDate) {
        if (!rows.hasNext() && lastVisitDate.hasTimedOut()) {
            // Engine will not drop the state from the state store automatically
            lastVisitDate.remove();
        }
        ArrayList<Row> res = new ArrayList<>();
        if (rows.hasNext()) {
            Row row = rows.next();
            HashMap<String, Object> updateMap = new HashMap<>();
            String date = DateUtil.timestampToDate(row.getAs("ts"));
            if (!lastVisitDate.exists() || !lastVisitDate.get().equals(date)) {
                updateMap.put("uv", 1);
                lastVisitDate.update(date);
                // Only one user view is contributed each day
                lastVisitDate.setTimeoutDuration("24 hours");
            }
            if (row.getAs("last_page_id") == null) {
                updateMap.put("sv", 1);
            }
            res.add(StreamUtil.updateRow(row, updateMap));
        }
        return res.iterator();
    }
}

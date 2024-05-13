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
import org.apache.spark.sql.types.DataTypes;
import org.liu.common.app.AppBase;
import org.liu.common.util.DateUtil;
import org.liu.common.util.StreamUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import static org.apache.spark.sql.functions.*;
import static org.liu.common.constant.Constant.*;

public class TrafficHomeDetailPageViewApp extends AppBase {
    public static void main(String[] args) {
        new TrafficHomeDetailPageViewApp().start(args, 4);
    }

    @Override
    public void etl(SparkSession spark, String[] args) {
        // Ingest streaming source from delta table
        Dataset<Row> source = deltaTableStream(TOPIC_LOG_PAGE)
                .select("mid", "page_id", "ts")
                .withColumn("uv", lit(0));

        // Data cleanse and transform
        Dataset<Row> homeSource = source.filter("page_id = 'home'").drop("page_id");
        Dataset<Row> goodDetailSource = source.filter("page_id = 'good_detail'").drop("page_id");
        homeSource = homeSource.groupByKey((MapFunction<Row, String>) r -> (String) r.getAs("mid"), Encoders.STRING())
                .flatMapGroupsWithState(this::stateHandler, OutputMode.Append(), Encoders.STRING(), RowEncoder.apply(homeSource.schema()), GroupStateTimeout.ProcessingTimeTimeout())
                .filter("uv > 0")
                .drop("mid");
        goodDetailSource = goodDetailSource.groupByKey((MapFunction<Row, String>) r -> (String) r.getAs("mid"), Encoders.STRING())
                .flatMapGroupsWithState(this::stateHandler, OutputMode.Append(), Encoders.STRING(), RowEncoder.apply(goodDetailSource.schema()), GroupStateTimeout.ProcessingTimeTimeout())
                .filter("uv > 0")
                .drop("mid");
        source = homeSource.withColumnRenamed("uv", "home_uv").withColumn("good_detail_uv", lit(0))
                .unionByName(goodDetailSource.withColumnRenamed("uv", "good_detail_uv").withColumn("home_uv", lit(0)));

        source = source
                .withWatermark("ts", "3 seconds")
                .groupBy(
                        window(col("ts"), "10 seconds")
                ).agg(sum("home_uv").as("homeUvCt"),
                        sum("good_detail_uv").as("goodDetailUvCt"))
                .withColumn("startTime", col("window.start").cast(DataTypes.StringType))
                .withColumn("endTime", col("window.end").cast(DataTypes.StringType))
                .withColumn("date", to_date(col("startTime"), "yyyy-MM-dd HH:mm:ss"))
                .drop("window");

        /*
        create table if not exists gmall.dws_traffic_home_detailPage_view_count
        (
            `startTime`   DATETIME,
            `endTime`     DATETIME,
            `date`        DATE NOT NULL,
            `homeUvCt` BIGINT REPLACE,
            `goodDetailUvCt` BIGINT REPLACE
        )
        engine = olap
        aggregate key (`startTime`,`endTime`,`date`)
        partition by LIST(`date`)(
        PARTITION `p20220608`  VALUES IN ("2022-06-08"),
        PARTITION `p20220609`  VALUES IN ("2022-06-09"),
        PARTITION `p20220610`  VALUES IN ("2022-06-10"),
        PARTITION `p20220618`  VALUES IN ("2022-06-18"))
        distributed by hash(`startTime`) buckets 10
        properties (
        "replication_num" = "1"
        );
         */
        streamToDorisTable(source, DWS_LAYER, DWS_TRAFFIC_HOME_DETAILPAGE_VIEW_COUNT);
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
            res.add(StreamUtil.updateRow(row, updateMap));
        }
        return res.iterator();
    }
}

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

public class UserLoginApp extends AppBase {
    public static void main(String[] args) {
        new UserLoginApp().start(args, 4);
    }

    @Override
    public void etl(SparkSession spark, String[] args) {
        Dataset<Row> source = deltaTableStream(TOPIC_LOG_PAGE)
                .filter("isnotnull(uid) AND isnotnull(ts) AND (isnull(last_page_id) OR last_page_id = 'login')")
                .select("uid", "ts")
                .withColumn("uuCt", lit(0))
                .withColumn("backCt", lit(0));

        source = source.groupByKey((MapFunction<Row, String>) row -> (String) row.getAs("uid"), Encoders.STRING())
                .flatMapGroupsWithState(this::stateHandler, OutputMode.Append(), Encoders.STRING(), RowEncoder.apply(source.schema()), GroupStateTimeout.NoTimeout())
                .drop("uid");

        source = source.withWatermark("ts", "3 seconds")
                .groupBy(window(col("ts"), "10 seconds"))
                .agg(
                        sum("uuCt").as("uuCt"),
                        sum("backCt").as("backCt")
                )
                .withColumn("startTime", col("window.start").cast(DataTypes.StringType))
                .withColumn("endTime", col("window.end").cast(DataTypes.StringType))
                .withColumn("date", to_date(col("startTime"), "yyyy-MM-dd HH:mm:ss"))
                .drop("window");

        /*
        create table if not exists gmall.dws_user_login
        (
            `startTime`   DATETIME,
            `endTime`     DATETIME,
            `date`        DATE NOT NULL,
            `uuCt` BIGINT REPLACE,
           `backCt` BIGINT REPLACE
)
        engine = olap
        aggregate key (`startTime`,`endTime`,`date`)
        partition by LIST(`date`)(
        PARTITION `p20220608`  VALUES IN ("2022-06-08"),
        PARTITION `p20220609`  VALUES IN ("2022-06-09"),
        PARTITION `p20220610`  VALUES IN ("2022-06-10"),
        PARTITION `p20220618`  VALUES IN ("2022-06-18")
)
        distributed by hash(`startTime`) buckets 10
        properties (
        "replication_num" = "1"
        );
         */
        streamToDorisTable(source, DWS_LAYER, DWS_USER_LOGIN);
    }

    private Iterator<Row> stateHandler(String uid, Iterator<Row> rows, GroupState<String> lastLoginDate) {
        ArrayList<Row> res = new ArrayList<>();
        while (rows.hasNext()) {
            Row row = rows.next();
            String date = DateUtil.timestampToDate(row.getAs("ts"));
            HashMap<String, Object> updateMap = new HashMap<>();
            if (lastLoginDate.exists()) {
                String preDate = lastLoginDate.get();
                if (!preDate.equals(date)) {
                    updateMap.put("uuCt", 1);
                    if (DateUtil.getDurationInDays(preDate, date) > 7) {
                        updateMap.put("backCt", 1);
                    }
                    lastLoginDate.update(date);
                }
            } else {
                updateMap.put("uuCt", 1);
                lastLoginDate.update(date);
            }
            if (!updateMap.isEmpty())
                res.add(StreamUtil.updateRow(row, updateMap));
        }
        return res.iterator();
    }
}

package org.liu.app;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
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
import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.*;
import static org.liu.common.constant.Constant.*;

public class UserLoginApp extends AppBase {
    public static void main(String[] args) {
        new UserLoginApp().start(args, 4);
    }

    @Override
    public void etl(SparkSession spark, String[] args) {
        Dataset<Row> source = deltaTableStream(spark, TOPIC_LOG_PAGE)
                .filter("isnotnull(uid) AND isnotnull(ts) AND (isnull(last_page_id) OR last_page_id = 'login')")
                .select("uid", "ts")
                .withColumn("uuCt", lit(0))
                .withColumn("backCt", lit(0));

        source = source.groupByKey((MapFunction<Row, String>) row -> (String) row.getAs("uid"), Encoders.STRING())
                .flatMapGroupsWithState(this::stateHandler, OutputMode.Append(), Encoders.STRING(), RowEncoder.apply(source.schema()), GroupStateTimeout.NoTimeout())
                .drop("uid")
                .filter("uuCt > 0 AND backCt > 0");

        source = source.withWatermark("ts", "3 seconds")
                .groupBy(
                        window(col("ts"), "10s")
                ).agg(
                        sum("uuCt").as("uuCt"),
                        sum("backCt").as("backCt")
                ).withColumn("date", date_format(col("ts"), "yyyy-MM-dd"));

//        streamToDeltaTable(source, DWS_LAYER, DWS_USER_LOGIN, "date");
//        deltaTableConsole(spark, DWS_USER_LOGIN);

        try {
            source.writeStream()
                    .format("console")
                    .start();
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        }
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
                    if (DateUtil.getDurationInSeconds(preDate, date) > 8 * 24 * 60 * 60) {
                        updateMap.put("backCt", 1);
                    }
                    lastLoginDate.update(date);
                }
            } else {
                updateMap.put("uuCt", 1);
                lastLoginDate.update(date);
            }
            res.add(StreamUtil.updateRow(row, updateMap));
        }
        return res.iterator();
    }
}

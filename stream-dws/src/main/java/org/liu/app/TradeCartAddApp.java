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

import java.util.ArrayList;
import java.util.Iterator;

import static org.apache.spark.sql.functions.*;
import static org.liu.common.constant.Constant.*;

public class TradeCartAddApp extends AppBase {
    public static void main(String[] args) {
        new TradeCartAddApp().start(args, 4);
    }

    @Override
    public void etl(SparkSession spark, String[] args) {
        Dataset<Row> source = deltaTableStream(spark, DWD_CART_INFO)
                .select("user_id", "create_time")
                .filter("isnotnull(user_id) AND isnotnull(create_time)")
                .withColumn("create_time", to_timestamp(col("create_time")));

        source = source.groupByKey((MapFunction<Row, String>) row -> (String) row.getAs("user_id"), Encoders.STRING())
                .flatMapGroupsWithState(this::stateHandler, OutputMode.Append(), Encoders.STRING(), RowEncoder.apply(source.schema()), GroupStateTimeout.NoTimeout());

        source = source.withWatermark("create_time", "5 seconds")
                .groupBy(window(col("create_time"), "10 seconds"))
                .agg(count("user_id").as("cartAddUuCt"))
                .withColumn("date", date_format(col("window.end"), "yyyy-MM-dd"));

        streamToDeltaTable(source, DWS_LAYER, DWS_TRADE_CART_ADD, "date");
        deltaTableConsole(spark, DWS_TRADE_CART_ADD);
    }

    private Iterator<Row> stateHandler(String user_id, Iterator<Row> rows, GroupState<String> lastCartAddDate) {
        ArrayList<Row> res = new ArrayList<>();
        while (rows.hasNext()) {
            Row row = rows.next();
            boolean shouldCount = false;
            String date = DateUtil.timestampToDate(row.getAs("create_time"));
            if (lastCartAddDate.exists()) {
                String preDate = lastCartAddDate.get();
                if (!preDate.equals(date)) {
                    lastCartAddDate.update(date);
                    shouldCount = true;
                }
            } else {
                lastCartAddDate.update(date);
                shouldCount = true;
            }
            if (shouldCount) res.add(row);
        }
        return res.iterator();
    }
}

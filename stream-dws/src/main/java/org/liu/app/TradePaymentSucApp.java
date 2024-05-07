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

import static org.apache.spark.sql.functions.*;
import static org.liu.common.constant.Constant.*;

public class TradePaymentSucApp extends AppBase {
    public static void main(String[] args) {
        new TradePaymentSucApp().start(args, 4);
    }

    @Override
    public void etl(SparkSession spark, String[] args) {
        Dataset<Row> source = deltaTableStream(spark, DWD_PAYMENT_INFO)
                .select("user_id", "payment_time")
                .na().drop(new String[]{"user_id", "payment_time"})
                .withColumn("payNewUserCnt", lit(0))
                .withColumn("payUuCnt", lit(0));

        source = source
                .groupByKey((MapFunction<Row, String>) row -> (String) row.getAs("user_id"), Encoders.STRING())
                .flatMapGroupsWithState(this::stateHandler, OutputMode.Append(), Encoders.STRING(), RowEncoder.apply(source.schema()), GroupStateTimeout.NoTimeout())
                .drop("user_id");

        source = source.withWatermark("payment_time", "5 seconds")
                .groupBy(window(col("payment_time"), "10 seconds"))
                .agg(sum("payNewUserCnt").as("payNewUserCnt"), sum("payUuCnt").as("payUuCnt"))
                .withColumn("date", date_format(col("window.end"), "yyyy-MM-dd"));

        streamToDeltaTable(source, DWS_LAYER, DWS_TRADE_PAYMENT_SUC, "date");
        deltaTableConsole(spark, DWS_TRADE_PAYMENT_SUC);
    }

    private Iterator<Row> stateHandler(String user_id, Iterator<Row> rows, GroupState<String> state) {
        ArrayList<Row> res = new ArrayList<>();
        while (rows.hasNext()) {
            Row row = rows.next();
            String curDate = DateUtil.timestampToDate(row.getAs("payment_time"));
            HashMap<String, Object> updateMap = new HashMap<>();
            if (!state.exists()) {
                updateMap.put("payNewUserCnt", 1);
                updateMap.put("payUuCnt", 1);
                state.update(curDate);
            } else if (!curDate.equals(state.get())) {
                updateMap.put("payUuCnt", 1);
                state.update(curDate);
            }
            if (!updateMap.isEmpty()) res.add(StreamUtil.updateRow(row, updateMap));
        }
        return res.iterator();
    }
}

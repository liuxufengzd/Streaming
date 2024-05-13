package org.liu.app;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
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

public class TradePaymentSucApp extends AppBase {
    public static void main(String[] args) {
        new TradePaymentSucApp().start(args, 4);
    }

    @Override
    public void etl(SparkSession spark, String[] args) {
        Dataset<Row> source = deltaTableStream(DWD_PAYMENT_INFO)
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
                .agg(sum("payNewUserCnt").as("payNewUserCnt"),
                        sum("payUuCnt").as("payUuCnt"))
                .withColumn("startTime", col("window.start").cast(DataTypes.StringType))
                .withColumn("endTime", col("window.end").cast(DataTypes.StringType))
                .withColumn("date", to_date(col("startTime"), "yyyy-MM-dd HH:mm:ss"))
                .drop("window");

        /*
        create table if not exists gmall.dws_trade_payment_suc
        (
            `startTime`   DATETIME,
            `endTime`     DATETIME,
            `date`        DATE NOT NULL,
            `payNewUserCnt` BIGINT REPLACE,
            `payUuCnt` BIGINT REPLACE
        )
        engine = olap
        aggregate key (`startTime`,`endTime`,`date`)
        partition by LIST(`date`)(
        PARTITION `p20220611`  VALUES IN ("2022-06-11"),
        PARTITION `p20220610`  VALUES IN ("2022-06-10")
        )
        distributed by hash(`startTime`) buckets 10
        properties (
        "replication_num" = "1"
        );
         */
        streamToDorisTable(source, DWS_LAYER, DWS_TRADE_PAYMENT_SUC);
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

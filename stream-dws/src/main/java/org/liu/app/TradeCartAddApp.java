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
        Dataset<Row> source = deltaTableStream(DWD_CART_INFO)
                .select("user_id", "create_time")
                .filter("isnotnull(user_id) AND isnotnull(create_time)")
                .withColumn("create_time", to_timestamp(col("create_time")));

        source = source.groupByKey((MapFunction<Row, String>) row -> (String) row.getAs("user_id"), Encoders.STRING())
                .flatMapGroupsWithState(this::stateHandler, OutputMode.Append(), Encoders.STRING(), RowEncoder.apply(source.schema()), GroupStateTimeout.NoTimeout());

        source = source.withWatermark("create_time", "5 seconds")
                .groupBy(window(col("create_time"), "10 seconds"))
                .agg(count("user_id").as("cartAddUuCt"))
                .withColumn("startTime", col("window.start").cast(DataTypes.StringType))
                .withColumn("endTime", col("window.end").cast(DataTypes.StringType))
                .withColumn("date", to_date(col("startTime"), "yyyy-MM-dd HH:mm:ss"))
                .drop("window");

        /*
        Create Doris table in advance:
        create table if not exists gmall.dws_trade_cart_add
        (
            `startTime`   DATETIME COMMENT '窗口起始时间',
            `endTime`     DATETIME COMMENT '窗口结束时间',
            `date`        DATE NOT NULL COMMENT '当天日期',
            `cartAddUuCt` BIGINT REPLACE COMMENT '加购独立用户数'
        )
        engine = olap
        aggregate key (`startTime`,`endTime`,`date`)
        partition by LIST(`date`)(
        PARTITION `p20220604`  VALUES IN ("2022-06-04"),
        PARTITION `p20220605`  VALUES IN ("2022-06-05"),
        PARTITION `p20220606`  VALUES IN ("2022-06-06"),
        PARTITION `p20220607`  VALUES IN ("2022-06-07"),
        PARTITION `p20220608`  VALUES IN ("2022-06-08"),
        PARTITION `p20220609`  VALUES IN ("2022-06-09"),
        PARTITION `p20220610`  VALUES IN ("2022-06-10"),
        PARTITION `p20220611`  VALUES IN ("2022-06-11"),
        PARTITION `p20220612`  VALUES IN ("2022-06-12"),
        PARTITION `p20220613`  VALUES IN ("2022-06-13"),
        PARTITION `p20220614`  VALUES IN ("2022-06-14"),
        PARTITION `p20220615`  VALUES IN ("2022-06-15"),
        PARTITION `p20220616`  VALUES IN ("2022-06-16"),
        PARTITION `p20220617`  VALUES IN ("2022-06-17"),
        PARTITION `p20220618`  VALUES IN ("2022-06-18"),
        PARTITION `p20220619`  VALUES IN ("2022-06-19"),
        PARTITION `p20220619`  VALUES IN ("2022-06-20")
        )
        distributed by hash(`startTime`) buckets 10
        properties (
        "replication_num" = "1"
        );
        We can create dynamic partition table in production:
         */
        streamToDorisTable(source, DWS_LAYER, DWS_TRADE_CART_ADD);
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

package org.liu.common.tool;

import io.delta.tables.DeltaTable;
import org.apache.spark.sql.SparkSession;
import org.liu.common.app.AppBase;

public class SeeDeltaTable extends AppBase {
    public static void main(String[] args) {
        new SeeDeltaTable().start(args, 4);
    }

    @Override
    public void etl(SparkSession spark, String[] args) {
        DeltaTable.forName(spark, args[0]).toDF().show();
    }
}

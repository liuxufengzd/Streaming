package org.liu.tool;

import org.apache.spark.sql.SparkSession;
import org.liu.common.app.AppBase;

public class DeltaOptimize extends AppBase {
    public static void main(String[] args) {
        new DeltaOptimize().start(args, 1);
    }

    @Override
    public void etl(SparkSession spark, String[] args) {
        spark.sql("OPTIMIZE " + args[0]);
    }
}

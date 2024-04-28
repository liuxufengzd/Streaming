package org.liu.tool;

import org.apache.spark.sql.SparkSession;
import org.liu.common.app.AppBase;

public class DeleteDeltaTable extends AppBase {
    public static void main(String[] args) {
        new DeleteDeltaTable().start(args, 1);
    }

    @Override
    public void etl(SparkSession spark, String[] args) {
        spark.sql("DROP TABLE IF EXISTS " + args[0]);
    }
}

package org.liu.app;

import org.apache.spark.sql.SparkSession;
import org.liu.common.app.AppBase;

public class DwdLogApp extends AppBase {
    public static void main(String[] args) {
        new DwdLogApp().start(args, 4);
    }

    @Override
    public void etl(SparkSession spark, String[] args) {

    }
}

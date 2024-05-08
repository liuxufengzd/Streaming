package org.liu.common.tool;

import io.delta.tables.DeltaTable;
import org.apache.spark.sql.SparkSession;
import org.liu.common.app.AppBase;

public class InsertToDeltaTable extends AppBase {
    public static void main(String[] args) {
        new InsertToDeltaTable().start(args, 4);
    }

    @Override
    public void etl(SparkSession spark, String[] args) {
        spark.sql("insert into stream.dwd_payment_info(date,user_id, payment_time, activity_id, activity_rule_id, coupon_id, order_id, order_price, payment_name, payment_type, province_id, sku_id, sku_name, sku_num, split_activity_amount, split_coupon_amount, split_total_amount, total_amount, user_name) VALUES('2022-06-11','9645','2022-06-11T00:00:00Z', null, null,null,null,null,null,null,null,null,null,null,null,null,null,null,null)");
    }
}

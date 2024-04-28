package org.liu.tool;

public class example {
    public static void main(String[] args) {
//        Dataset<Row> source = kafkaStream(spark, Constant.TOPIC_DB);
//        // Filter data
//        var jsonStruct = new StructType()
//                .add("key", DataTypes.StringType)
//                .add("value", DataTypes.StringType);
//        try {
//            source = source
//                    .filter((FilterFunction<Row>) row -> {
//                        var value = row.getString(1);
//                        JsonObject bean = new Gson().fromJson(value, JsonObject.class);
//                        var table = bean.get("table").getAsString();
//                        return "user_info".equals(table);
//                    }).map((MapFunction<Row, Row>) row -> {
//                        var value = row.getString(1);
//                        JsonObject bean = new Gson().fromJson(value, JsonObject.class);
//                        var table = bean.get("table");
//                        var database = bean.get("database");
//                        return RowFactory.create(row.getString(0), database + "-" + table);
//                    }, RowEncoder.apply(jsonStruct));
//
//            source.writeStream().format("console").start().awaitTermination();
//        } catch (StreamingQueryException | TimeoutException ignored) {
//        }
    }
}

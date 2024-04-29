package org.liu.common.util;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructType;
import org.json.JSONArray;
import org.liu.common.bean.dim.DimTableMeta;

import java.util.HashMap;
import java.util.Map;

import static org.apache.spark.sql.types.DataTypes.*;
import static org.liu.common.constant.Constant.*;

public class StreamUtil {
    public static DataType getDataType(String type) {
        switch (type.toLowerCase()) {
            case "string":
                return StringType;
            case "short":
                return ShortType;
            case "int":
                return IntegerType;
            case "long":
                return LongType;
            case "bool":
                return BooleanType;
            case "float":
                return FloatType;
            case "double":
                return DoubleType;
            case "datetime":
                return TimestampType;
            case "map":
                return new MapType();
            case "array":
                return new ArrayType();
            default:
                return new StructType();
        }
    }

    public static String getTablePath(String layer, String tableName) {
        return String.format("/user/%s/%s/%s", DELTA_DB, layer, tableName);
    }

    public static String getTableCheckpointPath(String layer, String tableName) {
        return String.format("/user/%s/%s/%s/%s", DELTA_DB, "checkpoints", layer, tableName);
    }

    public static DimTableMeta getDimMetaFromDimProcessRow(Row row) {
        var jsonArray = new JSONArray((String) row.getAs(DIM_PROCESS_SCHEMA));
        var structType = new StructType();
        for (int i = 0; i < jsonArray.length(); i++) {
            var jsonObject = jsonArray.getJSONObject(i);
            var name = jsonObject.getString("name");
            var type = jsonObject.getString("type");
            var nullable = type.endsWith("?");
            if (nullable) {
                type = type.substring(0, type.length() - 1);
            }
            structType = structType.add(name, StreamUtil.getDataType(type), nullable);
        }
        return new DimTableMeta(structType,
                row.getAs(DIM_PROCESS_SINK_TABLE),
                row.getAs(DIM_PROCESS_ROW_KEY),
                row.getAs(DIM_PROCESS_COLUMN_FAMILY),
                (int) row.getAs(DIM_PROCESS_TO_HBASE) != 0);
    }

    public static Row updateRow(Row row, Map<String, Object> columnValue) {
        HashMap<Integer, Object> map = new HashMap<>();
        columnValue.forEach((k, v) -> map.put(row.fieldIndex(k), v));
        Object[] objects = new Object[row.size()];
        map.forEach((k, v) -> objects[k] = v);
        for (int i = 0; i < row.size(); i++) {
            if (!map.containsKey(i)) {
                objects[i] = row.getAs(i);
            }
        }
        return RowFactory.create(objects);
    }
}

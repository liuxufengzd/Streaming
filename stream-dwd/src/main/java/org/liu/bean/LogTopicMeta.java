package org.liu.bean;

import lombok.Getter;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructType;
import org.json.JSONArray;
import org.json.JSONObject;
import org.liu.common.util.StreamUtil;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.spark.sql.types.DataTypes.*;

public class LogTopicMeta {
    private final String MAIN_SCHEMA = "MAIN";
    @Getter
    private final String topic;
    private static final Map<String, DataType> BASE_TYPES = new HashMap<>();
    private final Map<String, StructType> structDict = new HashMap<>();
    private final Map<String, JSONArray> structNodeMap = new HashMap<>();

    static {
        BASE_TYPES.put("short", ShortType);
        BASE_TYPES.put("integer", IntegerType);
        BASE_TYPES.put("long", LongType);
        BASE_TYPES.put("float", FloatType);
        BASE_TYPES.put("double", DoubleType);
        BASE_TYPES.put("string", StringType);
        BASE_TYPES.put("timestamp", TimestampType);
        BASE_TYPES.put("date", DateType);
        BASE_TYPES.put("boolean", BooleanType);
    }

    public LogTopicMeta(String configName) {
        InputStream stream = StreamUtil.class.getResourceAsStream("/" + configName + ".json");
        if (stream == null) throw new RuntimeException("cannot find the file:" + configName + ".json");
        try {
            JSONObject jsonObject = new JSONObject(new String(stream.readAllBytes()));
            this.topic = jsonObject.getString("topic");
            JSONArray schemaObject = jsonObject.getJSONArray("schema");
            for (int i = 0; i < schemaObject.length(); i++) {
                JSONObject schemaCell = schemaObject.getJSONObject(i);
                structNodeMap.put(schemaCell.getString("name"), schemaCell.getJSONArray("columns"));
            }
            getType(MAIN_SCHEMA);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private DataType getType(String rawType) {
        String type = rawType;
        if (rawType.endsWith("?")) {
            type = rawType.substring(0, rawType.length() - 1);
        }
        if (BASE_TYPES.containsKey(type)) {
            return BASE_TYPES.get(type);
        }
        Matcher arrayMatcher = Pattern.compile("^array<(.+)>$").matcher(type);
        if (arrayMatcher.find()) {
            return ArrayType.apply(getType(arrayMatcher.group(1).trim()));
        }
        Matcher mapMatcher = Pattern.compile("^map<(.+),(.+)>$").matcher(type);
        if (mapMatcher.find()) {
            return MapType.apply(getType(mapMatcher.group(1).trim()), getType(mapMatcher.group(2).trim()));
        }
        if (structDict.containsKey(type)) {
            return structDict.get(type);
        }
        StructType structType = new StructType();
        JSONArray jsonArray = structNodeMap.get(type);
        for (int i = 0; i < jsonArray.length(); i++) {
            String name = jsonArray.getJSONObject(i).getString("name");
            String childType = jsonArray.getJSONObject(i).getString("type");
            structType = structType.add(name, getType(childType), childType.endsWith("?"));
        }
        structDict.put(type, structType);
        return structType;
    }

    public StructType getStructType() {
        return getStructType(MAIN_SCHEMA);
    }

    public StructType getStructType(String name) {
        return structDict.get(name);
    }
}
package org.liu;

import org.json.JSONObject;
import org.liu.common.util.RedisUtil;
import redis.clients.jedis.Jedis;

public class Main {
    public static void main(String[] args) {
        Jedis client = RedisUtil.getClient();
        var dim = new JSONObject(client.get("1"));
        System.out.println(dim.getString("sku_name"));
    }
}

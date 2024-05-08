package org.liu.common.util;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.liu.common.constant.Constant;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.time.Duration;

public class RedisUtil {
    private final static JedisPool pool;

    static {
        /*
Resource Reuse: Connection pools allow for the reuse of expensive resources (like network connections). Instead of creating a new connection every time one is needed (which can be time-consuming and resource-intensive), connections are borrowed from the pool and returned after use, ready to be reused.
Improved Performance: By reusing existing connections, the application avoids the overhead of establishing a new connection every time one is needed. This can significantly improve the performance of your application, especially for workloads that require frequent short-lived connections.
Better Resource Management: Connection pools often provide additional features for managing resources, such as setting a maximum number of connections (setMaxTotal), controlling the number of idle connections (setMaxIdle and setMinIdle), and setting a maximum wait time for a connection (setMaxWait). These settings can help prevent resource exhaustion and improve the stability of your application.
Health Checking: Connection pools can automatically validate connections before they are borrowed (setTestOnBorrow), when they are returned to the pool (setTestOnReturn), or even right after they are created (setTestOnCreate). This ensures that your application always gets a healthy, usable connection.
         */
        GenericObjectPoolConfig<Jedis> config = new JedisPoolConfig();
        config.setMaxTotal(300);
        config.setMaxIdle(10);
        config.setMinIdle(2);
        config.setTestOnCreate(true);
        config.setTestOnBorrow(true);
        config.setTestOnReturn(true);
        config.setMaxWait(Duration.ofMillis(10 * 1000));

        pool = new JedisPool(config, "hadoop103", 6379);
    }

    public static Jedis getClient() {
        Jedis jedis = pool.getResource();
        jedis.select(0); // choose No.0 database here

        return jedis;
    }

    public static void closeClient(Jedis jedis) {
        if (jedis != null) {
            jedis.close(); // Return to connection pool
        }
    }

    public static String getKey(String tableName, String id) {
        return Constant.DATABASE + ":" + tableName + ":" + id;
    }
}

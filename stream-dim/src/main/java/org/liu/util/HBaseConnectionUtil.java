package org.liu.util;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;

public class HBaseConnectionUtil {
    public static Connection newConnection(){
        try {
            Connection connection = ConnectionFactory.createConnection();
            System.out.println("============== A new connection is created! ================");
            return connection;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static Admin getAdmin(Connection conn) {
        try {
            return conn.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static Table getTable(Connection conn, String database, String tableName) {
        try {
            return conn.getTable(TableName.valueOf(database, tableName));
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static void closeConnection(Connection conn) {
        try {
            if (conn != null)
                conn.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

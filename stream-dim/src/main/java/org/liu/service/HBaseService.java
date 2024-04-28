package org.liu.service;


import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.jetbrains.annotations.NotNull;
import org.liu.common.util.HBaseConnectionUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class HBaseService {
    private final Connection conn;

    public HBaseService(Connection conn) {
        this.conn = conn;
    }

    public void createDatabase(String name) {
        try (Admin admin = HBaseConnectionUtil.getAdmin(conn)) {
            admin.createNamespace(NamespaceDescriptor.create(name).build());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void createTableIfNotExist(String database, String table, String... columnFamilies) {
        if (columnFamilies.length == 0) {
            System.out.println("column family is empty");
            return;
        }
        if (tableExists(database, table)) {
            return;
        }

        Admin admin = HBaseConnectionUtil.getAdmin(conn);
        List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
        for (String columnFamily : columnFamilies) {
            ColumnFamilyDescriptor familyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(columnFamily))
                    .setMaxVersions(5).build();
            columnFamilyDescriptors.add(familyDescriptor);
        }

        var tableDescriptor = TableDescriptorBuilder.newBuilder(TableName.valueOf(database, table))
                .setColumnFamilies(columnFamilyDescriptors)
                .build();
        try {
            admin.createTable(tableDescriptor);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void putCell(String database, String tableName, String rowKey, String columnFamily, String columnName, String value) {
        try (Table table = HBaseConnectionUtil.getTable(conn, database, tableName)) {
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName), Bytes.toBytes(value));
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void insertRow(String database, String tableName, String rowKey, String columnFamily, Map<String, byte[]> columnValueMap) {
        try (Table table = HBaseConnectionUtil.getTable(conn, database, tableName)) {
            Put put = new Put(Bytes.toBytes(rowKey));
            columnValueMap.forEach((k, v) -> put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(k), v));
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void insertRows(String database, String tableName, List<Put> puts) {
        try (Table table = HBaseConnectionUtil.getTable(conn, database, tableName)) {
            table.put(puts);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public List<String> getColumns(String database, String tableName, String rowKey, Map<String, String> columnMap) {
        try (Table table = HBaseConnectionUtil.getTable(conn, database, tableName)) {
            Get get = new Get(Bytes.toBytes(rowKey));
            for (String column : columnMap.keySet()) {
                get.addColumn(Bytes.toBytes(columnMap.get(column)), Bytes.toBytes(column));
            }
            Result result = table.get(get);
            ArrayList<String> res = new ArrayList<>();
            for (Cell cell : result.rawCells()) {
                res.add(new String(CellUtil.cloneValue(cell)));
            }
            return res;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public List<List<String>> getRows(String database, String tableName, String startRowKey, String endRowKey, @NotNull Map<String, String> columnMap) throws IOException {
        try (Table table = HBaseConnectionUtil.getTable(conn, database, tableName)) {
            Scan scan = new Scan();
            scan.withStartRow(Bytes.toBytes(startRowKey));
            scan.withStopRow(Bytes.toBytes(endRowKey));
            for (String column : columnMap.keySet()) {
                scan.addColumn(Bytes.toBytes(columnMap.get(column)), Bytes.toBytes(column));
            }
            ArrayList<List<String>> res = new ArrayList<>();
            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                ArrayList<String> columns = new ArrayList<>();
                for (Cell cell : result.rawCells()) {
                    columns.add(new String(CellUtil.cloneValue(cell)));
                }
                res.add(columns);
            }
            return res;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public void getRowsWithFilter(String database, String tableName, String startRowKey, String endRowKey, String columnFamily, String column, String columnValue) throws IOException {
        try (Table table = HBaseConnectionUtil.getTable(conn, database, tableName)) {
            Scan scan = new Scan();
            scan.withStartRow(Bytes.toBytes(startRowKey));
            scan.withStopRow(Bytes.toBytes(endRowKey));

            var singleColumnValueFilter = new
                    SingleColumnValueFilter(
                    Bytes.toBytes(columnFamily),
                    Bytes.toBytes(column),
                    CompareOperator.EQUAL,
                    Bytes.toBytes(columnValue)
            );
            scan.setFilter(singleColumnValueFilter);
            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                Cell[] cells = result.rawCells();

                for (Cell cell : cells) {
                    System.out.print(new
                            String(CellUtil.cloneRow(cell)) + "-" + new
                            String(CellUtil.cloneFamily(cell)) + "-" + new
                            String(CellUtil.cloneQualifier(cell)) + "-" + new
                            String(CellUtil.cloneValue(cell)) + "\t");
                }
                System.out.println();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void deleteRow(String database, String tableName, String rowKey) {
        try (Table table = HBaseConnectionUtil.getTable(conn, database, tableName)) {
            table.delete(new Delete(Bytes.toBytes(rowKey)));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void deleteRows(String database, String tableName, List<Delete> deletes) {
        try (Table table = HBaseConnectionUtil.getTable(conn, database, tableName)) {
            table.delete(deletes);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private boolean tableExists(String database, String table) {
        try (Admin admin = HBaseConnectionUtil.getAdmin(conn)) {
            return admin.tableExists(TableName.valueOf(database, table));
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }
}

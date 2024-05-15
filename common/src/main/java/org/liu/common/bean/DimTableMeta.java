package org.liu.common.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;

@Data
@AllArgsConstructor
public class DimTableMeta implements Serializable {
    public StructType schema;
    public String sinkTable;
    public String rowKey;
    public String partitionBy;
    public String columnFamily;
    public boolean toHbase;
}

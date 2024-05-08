package org.liu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.Timestamp;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TradeProvinceOrderBean {
    String provinceId;
    String provinceName;
    long totalOrderNum;
    double totalOrderPrice;
    Timestamp startTime;
    Timestamp endTime;
    String date;
}

package org.liu.common.util;

import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class DateUtil {
    public static String convertLongToDate(long timestamp) {
        Date date = new Date(timestamp);
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        return formatter.format(date);
    }

    public static String addNDaysToDate(String date, int days){
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        return LocalDate.parse(date, formatter).plusDays(days).format(formatter);
    }

    public static String getDate(String timestamp){
        DateTimeFormatter formatter1 = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        DateTimeFormatter formatter2 = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        return LocalDate.parse(timestamp, formatter1).format(formatter2);
    }
}

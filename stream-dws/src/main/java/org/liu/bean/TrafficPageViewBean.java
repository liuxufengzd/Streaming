package org.liu.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class TrafficPageViewBean {
    // 窗口起始时间
    private String startTime;
    // 窗口结束时间
    private String endTime;
    // 当天日期
    private String date;
    // app 版本号
    private String vc;
    // 渠道
    private String ch;
    // 地区
    private String ar;
    // 新老访客状态标记
    private String isNew ;

    // 独立访客数
    private long uvCt;
    // 会话数
    private long svCt;
    // 页面浏览数
    private long pvCt;
    // 累计访问时长
    private long durSum;
    // 时间戳
//    @JSONField(serialize = false)  // 要不要序列化这个字段
    private long ts;
//    @JSONField(serialize = false)  // 要不要序列化这个字段
    private String sid;
}

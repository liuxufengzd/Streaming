package org.liu.bean.state;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class DeviceState {
    public String firstVisitDate;
    public String lastVisitTime;
}

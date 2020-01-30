package com.stevenchennet.net.cogroup;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MinuteMetric {
    private String billId;
    private long uptTimeStart;
    private long uptTimeEnd;
    private double maxSoc;
    private double maxT;
    private String maxTCode;
    private double maxKwhMeter;
    private double minuteKwh;
    private double maxDirectPower;
    private double maxDirectCurrent;
    private double maxDirectVoltage;
}

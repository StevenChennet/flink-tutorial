package com.stevenchennet.net.customtimer;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MinuteMetric {
    private String id;
    private long uptTimeStart;
    private long uptTimeEnd;
    private double maxSoc;
    private double maxT;
    private String maxTCode;
    private double maxKwhMeter;
    private double minuteKwh;
}

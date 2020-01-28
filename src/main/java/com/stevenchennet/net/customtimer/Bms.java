package com.stevenchennet.net.customtimer;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class Bms {
    private String id;
    private String uptTimeStr;
    private long uptTime;
    private double soc;
    private double tMax;
    private String tMaxCode;
    private double kwhMeter;
}

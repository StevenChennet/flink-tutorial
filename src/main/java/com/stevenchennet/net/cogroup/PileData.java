package com.stevenchennet.net.cogroup;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class PileData {
    private long uptTime;
    private String id;
    private String billId;
    private double directPower;
    private double directCurrent;
    private double directVoltage;
}

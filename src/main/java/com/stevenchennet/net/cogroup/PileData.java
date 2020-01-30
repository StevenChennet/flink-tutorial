package com.stevenchennet.net.cogroup;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class PileData {
    private long uptTime;
    private String id;
    private String billId;
    private double DirectPower;
    private double DirectCurrent;
    private double DirectVoltage;
}

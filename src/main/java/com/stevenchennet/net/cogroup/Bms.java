package com.stevenchennet.net.cogroup;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class Bms {
    private long uptTime;
    private String id;
    private String billId;
    private double soc;
    private double tMax;
    private String tMaxCode;
}

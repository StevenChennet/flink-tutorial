package com.stevenchennet.net.cogroup;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class KwhMeter {
    private long uptTime;
    private String id;
    private String billId;
    private double kwhMeter;
}

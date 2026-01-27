package com.wikimedia.streams.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BotRatioStat {

    private long botEdits;
    private long humanEdits;
    private double botPercentage;
    private long windowStart;
}

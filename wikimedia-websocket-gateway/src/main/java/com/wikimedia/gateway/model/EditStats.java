package com.wikimedia.gateway.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class EditStats {

    private long windowStart;
    private long windowEnd;
    private long totalEdits;
    private long uniqueUsers;
    private double avgEditSize;
    private double editsPerMinute;
}

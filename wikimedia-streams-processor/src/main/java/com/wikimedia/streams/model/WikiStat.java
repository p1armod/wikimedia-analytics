package com.wikimedia.streams.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class WikiStat {

    private String wiki;

    private long edits;

    private double percentage;
}

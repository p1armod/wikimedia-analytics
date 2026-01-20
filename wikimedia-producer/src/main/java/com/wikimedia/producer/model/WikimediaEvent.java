package com.wikimedia.producer.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;


@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class WikimediaEvent {

    private String id;

    private String type;

    private String wiki;

    private String title;

    private String user;

    private boolean bot;

    @JsonProperty("server_name")
    private String serverName;

    private long timestamp;

    @JsonProperty("length")
    private LengthInfo length;

    private String comment;

    private Map<String, Object> meta;

    
    public int getLengthNew() {
        if (length != null && length.getNewLength() != null) {
            return length.getNewLength();
        }
        return 0;
    }

    
    public int getLengthOld() {
        if (length != null && length.getOldLength() != null) {
            return length.getOldLength();
        }
        return 0;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class LengthInfo {
        @JsonProperty("new")
        private Integer newLength;

        @JsonProperty("old")
        private Integer oldLength;
    }
}

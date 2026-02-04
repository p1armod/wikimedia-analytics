package com.wikimedia.streams.topology;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.wikimedia.streams.model.WikiStat;
import com.wikimedia.streams.model.WikimediaEvent;
import com.wikimedia.streams.serde.JsonSerde;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


@Slf4j
// Define Kafka Streams processing topology
public class TopWikisTopology {

    private static final String OUTPUT_TOPIC = "wikimedia.top-wikis";
    private static final Duration WINDOW_SIZE = Duration.ofMinutes(1);
    private static final Duration GRACE_PERIOD = Duration.ofSeconds(10);
    private static final int TOP_N = 10;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    
    public static void build(KStream<String, WikimediaEvent> sourceStream) {
        log.info("Building TopWikisTopology — top {} wikis per {}-minute window", TOP_N, WINDOW_SIZE.toMinutes());

        JsonSerde<WikiCountAccumulator> accSerde = new JsonSerde<>(WikiCountAccumulator.class);

        sourceStream

                .selectKey((key, value) -> "all-wikis")
                .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(WikimediaEvent.class)))
                .windowedBy(TimeWindows.ofSizeAndGrace(WINDOW_SIZE, GRACE_PERIOD))
                .aggregate(
                        WikiCountAccumulator::new,
                        (key, event, accumulator) -> {
                            String wiki = event.getServerName() != null ? event.getServerName() : "unknown";
                            accumulator.counts.merge(wiki, 1L, Long::sum);
                            accumulator.totalEdits++;
                            return accumulator;
                        },
                        Materialized.<String, WikiCountAccumulator, WindowStore<Bytes, byte[]>>as("top-wikis-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(accSerde)
                )
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
                .toStream()
                .map((windowedKey, accumulator) -> {

                    List<WikiStat> topWikis = accumulator.counts.entrySet().stream()
                            .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
                            .limit(TOP_N)
                            .map(entry -> WikiStat.builder()
                                    .wiki(entry.getKey())
                                    .edits(entry.getValue())
                                    .percentage(accumulator.totalEdits > 0
                                            ? (double) entry.getValue() / accumulator.totalEdits * 100.0
                                            : 0.0)
                                    .build())
                            .toList();

                    log.info("Top wikis window closed: {} wikis tracked, top={} with {} edits",
                            accumulator.counts.size(),
                            topWikis.isEmpty() ? "none" : topWikis.get(0).getWiki(),
                            topWikis.isEmpty() ? 0 : topWikis.get(0).getEdits());

                    try {
                        String jsonList = MAPPER.writeValueAsString(topWikis);
                        return KeyValue.pair("top-wikis", jsonList);
                    } catch (Exception e) {
                        log.error("Failed to serialize top wikis list", e);
                        return KeyValue.pair("top-wikis", "[]");
                    }
                })
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));
    }

    
    @lombok.Data
    @lombok.NoArgsConstructor
    public static class WikiCountAccumulator {
        Map<String, Long> counts = new HashMap<>();
        long totalEdits = 0;
    }
}

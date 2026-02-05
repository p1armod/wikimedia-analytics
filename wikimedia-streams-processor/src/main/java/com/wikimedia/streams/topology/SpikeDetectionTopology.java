package com.wikimedia.streams.topology;

import com.wikimedia.streams.model.AlertEvent;
import com.wikimedia.streams.model.WikimediaEvent;
import com.wikimedia.streams.serde.JsonSerde;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;

import java.time.Duration;
import java.util.LinkedList;
import java.util.UUID;


@Slf4j
// Define Kafka Streams processing topology
public class SpikeDetectionTopology {

    private static final String OUTPUT_TOPIC = "wikimedia.alerts";
    private static final Duration WINDOW_SIZE = Duration.ofMinutes(1);
    private static final Duration GRACE_PERIOD = Duration.ofSeconds(30);
    private static final double WARNING_THRESHOLD = 2.5;
    private static final double CRITICAL_THRESHOLD = 4.0;
    private static final int BASELINE_WINDOW_COUNT = 5;

    
    public static void build(KStream<String, WikimediaEvent> sourceStream) {
        log.info("Building SpikeDetectionTopology — thresholds: WARNING={}x, CRITICAL={}x",
                WARNING_THRESHOLD, CRITICAL_THRESHOLD);

        JsonSerde<SpikeAccumulator> accSerde = new JsonSerde<>(SpikeAccumulator.class);
        JsonSerde<AlertEvent> alertSerde = new JsonSerde<>(AlertEvent.class);

        sourceStream

                .selectKey((key, event) -> event.getServerName() != null ? event.getServerName() : "unknown")
                .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(WikimediaEvent.class)))
                .windowedBy(TimeWindows.ofSizeAndGrace(WINDOW_SIZE, GRACE_PERIOD))
                .aggregate(
                        SpikeAccumulator::new,
                        (wiki, event, accumulator) -> {
                            accumulator.currentCount++;
                            return accumulator;
                        },
                        Materialized.<String, SpikeAccumulator, WindowStore<Bytes, byte[]>>as("spike-detection-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(accSerde)
                )
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
                .toStream()
                .flatMap((windowedKey, accumulator) -> {
                    String wiki = windowedKey.key();
                    long currentCount = accumulator.currentCount;

                    double baseline = 0;
                    if (!accumulator.previousCounts.isEmpty()) {
                        baseline = accumulator.previousCounts.stream()
                                .mapToLong(Long::longValue)
                                .average()
                                .orElse(0.0);
                    }

                    accumulator.previousCounts.addLast(currentCount);
                    if (accumulator.previousCounts.size() > BASELINE_WINDOW_COUNT) {
                        accumulator.previousCounts.removeFirst();
                    }

                    java.util.List<KeyValue<String, AlertEvent>> alerts = new java.util.ArrayList<>();

                    if (baseline > 0 && currentCount > WARNING_THRESHOLD * baseline) {
                        String severity = currentCount > CRITICAL_THRESHOLD * baseline ? "CRITICAL" : "WARNING";

                        AlertEvent alert = AlertEvent.builder()
                                .alertId(UUID.randomUUID().toString())
                                .wiki(wiki)
                                .severity(severity)
                                .editCount(currentCount)
                                .baseline(baseline)
                                .detectedAt(System.currentTimeMillis())
                                .message(String.format("%s: %s had %d edits/min (baseline: %.1f) — %.1fx normal",
                                        severity, wiki, currentCount, baseline, currentCount / baseline))
                                .build();

                        log.warn("Spike detected: {}", alert.getMessage());
                        alerts.add(KeyValue.pair(wiki, alert));
                    }

                    return alerts;
                })
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), alertSerde));
    }

    
    @lombok.Data
    @lombok.NoArgsConstructor
    public static class SpikeAccumulator {
        long currentCount = 0;
        LinkedList<Long> previousCounts = new LinkedList<>();
    }
}

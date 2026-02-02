package com.wikimedia.streams.topology;

import com.wikimedia.streams.model.EditStats;
import com.wikimedia.streams.model.WikimediaEvent;
import com.wikimedia.streams.serde.JsonSerde;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;


@Slf4j
// Define Kafka Streams processing topology
public class EditStatsTopology {

    private static final String OUTPUT_TOPIC = "wikimedia.stats";
    private static final Duration WINDOW_SIZE = Duration.ofMinutes(1);
    private static final Duration GRACE_PERIOD = Duration.ofSeconds(10);

    
    public static void build(KStream<String, WikimediaEvent> sourceStream) {
        log.info("Building EditStatsTopology with {}-minute window", WINDOW_SIZE.toMinutes());

        JsonSerde<EditStatsAccumulator> accumulatorSerde = new JsonSerde<>(EditStatsAccumulator.class);
        JsonSerde<EditStats> statsSerde = new JsonSerde<>(EditStats.class);

        sourceStream

                .selectKey((key, value) -> "global")

                .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(WikimediaEvent.class)))

                .windowedBy(TimeWindows.ofSizeAndGrace(WINDOW_SIZE, GRACE_PERIOD))

                .aggregate(
                        EditStatsAccumulator::new,
                        (key, event, accumulator) -> {
                            accumulator.totalEdits++;
                            accumulator.users.add(event.getUser() != null ? event.getUser() : "anonymous");
                            int editSize = Math.abs(event.getLengthNew() - event.getLengthOld());
                            accumulator.totalEditSize += editSize;
                            return accumulator;
                        },
                        Materialized.<String, EditStatsAccumulator, WindowStore<Bytes, byte[]>>as("edit-stats-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(accumulatorSerde)
                )

                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))

                .toStream()
                .map((windowedKey, accumulator) -> {
                    long windowStart = windowedKey.window().start();
                    long windowEnd = windowedKey.window().end();

                    EditStats stats = EditStats.builder()
                            .windowStart(windowStart)
                            .windowEnd(windowEnd)
                            .totalEdits(accumulator.totalEdits)
                            .uniqueUsers(accumulator.users.size())
                            .avgEditSize(accumulator.totalEdits > 0
                                    ? (double) accumulator.totalEditSize / accumulator.totalEdits
                                    : 0.0)
                            .editsPerMinute(accumulator.totalEdits) // 1-minute window, so count = EPM
                            .build();

                    log.info("Window [{} - {}]: {} edits, {} unique users, {} avg edit size",
                            windowStart, windowEnd, stats.getTotalEdits(),
                            stats.getUniqueUsers(), stats.getAvgEditSize());

                    return KeyValue.pair("stats", stats);
                })
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), statsSerde));
    }

    
    @lombok.Data
    @lombok.NoArgsConstructor
    public static class EditStatsAccumulator {
        long totalEdits = 0;
        Set<String> users = new HashSet<>();
        long totalEditSize = 0;
    }
}

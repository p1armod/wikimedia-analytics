package com.wikimedia.streams.topology;

import com.wikimedia.streams.model.BotRatioStat;
import com.wikimedia.streams.model.WikimediaEvent;
import com.wikimedia.streams.serde.JsonSerde;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;

import java.time.Duration;


@Slf4j
// Define Kafka Streams processing topology
public class BotRatioTopology {

    private static final String OUTPUT_TOPIC = "wikimedia.bot-ratio";
    private static final Duration WINDOW_SIZE = Duration.ofMinutes(5);
    private static final Duration GRACE_PERIOD = Duration.ofSeconds(30);

    
    public static void build(KStream<String, WikimediaEvent> sourceStream) {
        log.info("Building BotRatioTopology with {}-minute window", WINDOW_SIZE.toMinutes());

        JsonSerde<BotHumanAccumulator> accSerde = new JsonSerde<>(BotHumanAccumulator.class);
        JsonSerde<BotRatioStat> statSerde = new JsonSerde<>(BotRatioStat.class);

        sourceStream
                .selectKey((key, value) -> "bot-ratio")
                .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(WikimediaEvent.class)))
                .windowedBy(TimeWindows.ofSizeAndGrace(WINDOW_SIZE, GRACE_PERIOD))
                .aggregate(
                        BotHumanAccumulator::new,
                        (key, event, accumulator) -> {
                            if (event.isBot()) {
                                accumulator.botEdits++;
                            } else {
                                accumulator.humanEdits++;
                            }
                            return accumulator;
                        },
                        Materialized.<String, BotHumanAccumulator, WindowStore<Bytes, byte[]>>as("bot-ratio-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(accSerde)
                )
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
                .toStream()
                .map((windowedKey, accumulator) -> {
                    long total = accumulator.botEdits + accumulator.humanEdits;
                    double botPct = total > 0
                            ? (double) accumulator.botEdits / total * 100.0
                            : 0.0;

                    BotRatioStat stat = BotRatioStat.builder()
                            .botEdits(accumulator.botEdits)
                            .humanEdits(accumulator.humanEdits)
                            .botPercentage(botPct)
                            .windowStart(windowedKey.window().start())
                            .build();

                    log.info("Bot ratio window: {} bot edits, {} human edits, {}% bot",
                            stat.getBotEdits(), stat.getHumanEdits(), stat.getBotPercentage());

                    return KeyValue.pair("bot-ratio", stat);
                })
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), statSerde));
    }

    
    @lombok.Data
    @lombok.NoArgsConstructor
    public static class BotHumanAccumulator {
        long botEdits = 0;
        long humanEdits = 0;
    }
}

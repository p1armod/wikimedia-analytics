package com.wikimedia.streams.config;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.wikimedia.streams.model.*;
import com.wikimedia.streams.serde.JsonSerde;
import com.wikimedia.streams.topology.BotRatioTopology;
import com.wikimedia.streams.topology.EditStatsTopology;
import com.wikimedia.streams.topology.SpikeDetectionTopology;
import com.wikimedia.streams.topology.TopWikisTopology;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.util.Properties;


@Slf4j
@Configuration
public class KafkaStreamsConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.streams.application-id}")
    private String applicationId;

    private KafkaStreams kafkaStreams;

    // Configure Spring Bean
    @Bean
    public ObjectMapper objectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        return mapper;
    }

    @PostConstruct
    public void startStreams() {
        log.info("Starting Kafka Streams application: {}", applicationId);

        StreamsBuilder builder = new StreamsBuilder();
        JsonSerde<WikimediaEvent> eventSerde = new JsonSerde<>(WikimediaEvent.class);

        KStream<String, WikimediaEvent> sourceStream = builder.stream(
                "wikimedia.recentchange",
                Consumed.with(Serdes.String(), eventSerde)
        );

        EditStatsTopology.build(sourceStream);
        TopWikisTopology.build(sourceStream);
        BotRatioTopology.build(sourceStream);
        SpikeDetectionTopology.build(sourceStream);

        Topology topology = builder.build();
        log.info("Kafka Streams topology:\n{}", topology.describe());

        Properties props = new Properties();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 10485760);

        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                org.apache.kafka.streams.errors.LogAndContinueExceptionHandler.class.getName());

        kafkaStreams = new KafkaStreams(topology, props);

        kafkaStreams.setUncaughtExceptionHandler((thread, exception) -> {
            log.error("Uncaught exception in Kafka Streams thread {}: {}", thread.getName(), exception.getMessage(), exception);
        });

        kafkaStreams.start();
        log.info("Kafka Streams started successfully");
    }

    @PreDestroy
    public void stopStreams() {
        if (kafkaStreams != null) {
            log.info("Shutting down Kafka Streams");
            kafkaStreams.close();
        }
    }
}

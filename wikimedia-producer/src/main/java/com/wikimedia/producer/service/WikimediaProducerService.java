package com.wikimedia.producer.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.wikimedia.producer.model.WikimediaEvent;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;


@Slf4j
@Service
public class WikimediaProducerService {

    private final WebClient webClient;
    private final KafkaTemplate<String, WikimediaEvent> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Value("${wikimedia.stream.url}")
    private String streamUrl;

    @Value("${wikimedia.kafka.topic:wikimedia.recentchange}")
    private String topicName;

    private final AtomicLong eventCount = new AtomicLong(0);

    public WikimediaProducerService(WebClient webClient,
                                     KafkaTemplate<String, WikimediaEvent> kafkaTemplate,
                                     ObjectMapper objectMapper) {
        this.webClient = webClient;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    
    @PostConstruct
    public void startStream() {
        log.info("Starting Wikimedia SSE stream consumer from: {}", streamUrl);

        webClient.get()
                .uri(streamUrl)
                .retrieve()
                .bodyToFlux(String.class)
                .filter(line -> line != null && !line.isEmpty())
                .doOnError(error -> log.error("SSE stream error: {}", error.getMessage()))
                .retryWhen(Retry.backoff(Long.MAX_VALUE, Duration.ofSeconds(1))
                        .maxBackoff(Duration.ofSeconds(30))
                        .doBeforeRetry(signal -> log.warn("Reconnecting to SSE stream, attempt #{}", signal.totalRetries() + 1)))
                .subscribe(this::processEvent);
    }

    
    private void processEvent(String eventData) {
        try {


            String jsonData = eventData;
            if (eventData.startsWith("data:")) {
                jsonData = eventData.substring(5).trim();
            }

            if (jsonData.isEmpty() || !jsonData.startsWith("{")) {
                return;
            }

            WikimediaEvent event = objectMapper.readValue(jsonData, WikimediaEvent.class);


            String key = event.getServerName() != null ? event.getServerName() : "unknown";

            kafkaTemplate.send(topicName, key, event);

            long count = eventCount.incrementAndGet();
            if (count % 1000 == 0) {
                log.info("Published {} events to Kafka. Latest: wiki={}, type={}, user={}",
                        count, event.getWiki(), event.getType(), event.getUser());
            }

        } catch (Exception e) {

            log.debug("Failed to process event: {}. Error: {}", 
                    eventData.substring(0, Math.min(200, eventData.length())), e.getMessage());
        }
    }
}

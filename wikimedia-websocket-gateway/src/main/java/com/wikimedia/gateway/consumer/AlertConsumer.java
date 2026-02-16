package com.wikimedia.gateway.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.wikimedia.gateway.model.AlertEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;


@Slf4j
@Component
public class AlertConsumer {

    private final SimpMessagingTemplate messagingTemplate;
    private final ObjectMapper objectMapper;

    private static final String STOMP_DESTINATION = "/topic/alerts";

    public AlertConsumer(SimpMessagingTemplate messagingTemplate,
                         ObjectMapper objectMapper) {
        this.messagingTemplate = messagingTemplate;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "wikimedia.alerts", groupId = "wikimedia-gateway")
    public void consume(String message) {
        try {
            AlertEvent alert = objectMapper.readValue(message, AlertEvent.class);

            messagingTemplate.convertAndSend(STOMP_DESTINATION, alert);

            log.warn("Alert broadcast: [{}] {} — {} edits (baseline: {})",
                    alert.getSeverity(), alert.getWiki(),
                    alert.getEditCount(), alert.getBaseline());

        } catch (Exception e) {
            log.error("Failed to process alert message: {}", e.getMessage());
        }
    }
}

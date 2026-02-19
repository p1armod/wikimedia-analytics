package com.wikimedia.gateway;

import com.wikimedia.gateway.model.EditStats;
import com.wikimedia.gateway.model.WikiStat;
import com.wikimedia.gateway.model.BotRatioStat;
import com.wikimedia.gateway.model.AlertEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;


class WikimediaGatewayApplicationTests {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void testEditStatsRoundTrip() throws Exception {
        EditStats stats = EditStats.builder()
                .windowStart(1700000000000L)
                .windowEnd(1700000060000L)
                .totalEdits(200)
                .uniqueUsers(55)
                .avgEditSize(300.0)
                .editsPerMinute(200)
                .build();

        String json = objectMapper.writeValueAsString(stats);
        EditStats result = objectMapper.readValue(json, EditStats.class);

        assertEquals(200, result.getTotalEdits());
        assertEquals(55, result.getUniqueUsers());
    }

    @Test
    void testWikiStatRoundTrip() throws Exception {
        WikiStat stat = WikiStat.builder()
                .wiki("en.wikipedia.org")
                .edits(1000)
                .percentage(45.5)
                .build();

        String json = objectMapper.writeValueAsString(stat);
        WikiStat result = objectMapper.readValue(json, WikiStat.class);

        assertEquals("en.wikipedia.org", result.getWiki());
        assertEquals(1000, result.getEdits());
    }

    @Test
    void testBotRatioStatRoundTrip() throws Exception {
        BotRatioStat stat = BotRatioStat.builder()
                .botEdits(100)
                .humanEdits(400)
                .botPercentage(20.0)
                .windowStart(1700000000000L)
                .build();

        String json = objectMapper.writeValueAsString(stat);
        BotRatioStat result = objectMapper.readValue(json, BotRatioStat.class);

        assertEquals(100, result.getBotEdits());
        assertEquals(20.0, result.getBotPercentage(), 0.01);
    }

    @Test
    void testAlertEventRoundTrip() throws Exception {
        AlertEvent alert = AlertEvent.builder()
                .alertId("uuid-123")
                .wiki("en.wikipedia.org")
                .severity("CRITICAL")
                .editCount(500)
                .baseline(100.0)
                .detectedAt(System.currentTimeMillis())
                .message("CRITICAL spike on en.wikipedia.org")
                .build();

        String json = objectMapper.writeValueAsString(alert);
        AlertEvent result = objectMapper.readValue(json, AlertEvent.class);

        assertEquals("CRITICAL", result.getSeverity());
        assertEquals("en.wikipedia.org", result.getWiki());
        assertEquals(500, result.getEditCount());
    }
}

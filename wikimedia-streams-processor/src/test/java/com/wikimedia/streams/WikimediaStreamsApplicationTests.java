package com.wikimedia.streams;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.wikimedia.streams.model.EditStats;
import com.wikimedia.streams.model.WikiStat;
import com.wikimedia.streams.model.AlertEvent;
import com.wikimedia.streams.model.BotRatioStat;
import com.wikimedia.streams.serde.JsonSerde;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;


class WikimediaStreamsApplicationTests {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void testEditStatsSerialization() throws Exception {
        EditStats stats = EditStats.builder()
                .windowStart(1700000000000L)
                .windowEnd(1700000060000L)
                .totalEdits(150)
                .uniqueUsers(45)
                .avgEditSize(250.5)
                .editsPerMinute(150)
                .build();

        String json = objectMapper.writeValueAsString(stats);
        assertNotNull(json);
        assertTrue(json.contains("\"totalEdits\":150"));

        EditStats deserialized = objectMapper.readValue(json, EditStats.class);
        assertEquals(150, deserialized.getTotalEdits());
        assertEquals(45, deserialized.getUniqueUsers());
        assertEquals(250.5, deserialized.getAvgEditSize(), 0.01);
    }

    @Test
    void testWikiStatSerialization() throws Exception {
        WikiStat stat = WikiStat.builder()
                .wiki("en.wikipedia.org")
                .edits(500)
                .percentage(33.3)
                .build();

        String json = objectMapper.writeValueAsString(stat);
        WikiStat deserialized = objectMapper.readValue(json, WikiStat.class);

        assertEquals("en.wikipedia.org", deserialized.getWiki());
        assertEquals(500, deserialized.getEdits());
        assertEquals(33.3, deserialized.getPercentage(), 0.01);
    }

    @Test
    void testAlertEventSerialization() throws Exception {
        AlertEvent alert = AlertEvent.builder()
                .alertId("test-uuid-123")
                .wiki("en.wikipedia.org")
                .severity("WARNING")
                .editCount(300)
                .baseline(100.0)
                .detectedAt(System.currentTimeMillis())
                .message("WARNING: en.wikipedia.org had 300 edits/min")
                .build();

        String json = objectMapper.writeValueAsString(alert);
        AlertEvent deserialized = objectMapper.readValue(json, AlertEvent.class);

        assertEquals("WARNING", deserialized.getSeverity());
        assertEquals(300, deserialized.getEditCount());
        assertEquals(100.0, deserialized.getBaseline(), 0.01);
    }

    @Test
    void testBotRatioStatSerialization() throws Exception {
        BotRatioStat stat = BotRatioStat.builder()
                .botEdits(200)
                .humanEdits(800)
                .botPercentage(20.0)
                .windowStart(1700000000000L)
                .build();

        String json = objectMapper.writeValueAsString(stat);
        BotRatioStat deserialized = objectMapper.readValue(json, BotRatioStat.class);

        assertEquals(200, deserialized.getBotEdits());
        assertEquals(800, deserialized.getHumanEdits());
        assertEquals(20.0, deserialized.getBotPercentage(), 0.01);
    }

    @Test
    void testJsonSerdeRoundTrip() {
        JsonSerde<EditStats> serde = new JsonSerde<>(EditStats.class);

        EditStats original = EditStats.builder()
                .windowStart(1700000000000L)
                .windowEnd(1700000060000L)
                .totalEdits(99)
                .uniqueUsers(33)
                .avgEditSize(100.0)
                .editsPerMinute(99)
                .build();

        byte[] bytes = serde.serializer().serialize("test-topic", original);
        assertNotNull(bytes);
        assertTrue(bytes.length > 0);

        EditStats deserialized = serde.deserializer().deserialize("test-topic", bytes);
        assertNotNull(deserialized);
        assertEquals(99, deserialized.getTotalEdits());
        assertEquals(33, deserialized.getUniqueUsers());
    }

    @Test
    void testJsonSerdeHandlesNull() {
        JsonSerde<EditStats> serde = new JsonSerde<>(EditStats.class);

        assertNull(serde.serializer().serialize("test", null));
        assertNull(serde.deserializer().deserialize("test", null));
        assertNull(serde.deserializer().deserialize("test", new byte[0]));
    }
}

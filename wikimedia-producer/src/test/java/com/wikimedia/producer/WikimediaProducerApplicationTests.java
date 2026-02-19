package com.wikimedia.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.wikimedia.producer.model.WikimediaEvent;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.web.reactive.function.client.WebClient;

import static org.junit.jupiter.api.Assertions.*;


@SpringBootTest
class WikimediaProducerApplicationTests {

    @MockBean
    private KafkaTemplate<String, WikimediaEvent> kafkaTemplate;

    @MockBean
    private WebClient webClient;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    void contextLoads() {

        assertNotNull(objectMapper);
    }

    @Test
    void testEventDeserialization() throws Exception {

        String json = """
                {
                    "id": "12345",
                    "type": "edit",
                    "wiki": "enwiki",
                    "title": "Test Article",
                    "user": "TestUser",
                    "bot": false,
                    "server_name": "en.wikipedia.org",
                    "timestamp": 1700000000,
                    "length": {
                        "new": 5000,
                        "old": 4800
                    },
                    "comment": "Fixed typo",
                    "meta": {
                        "dt": "2024-01-01T00:00:00Z",
                        "domain": "en.wikipedia.org"
                    }
                }
                """;

        WikimediaEvent event = objectMapper.readValue(json, WikimediaEvent.class);

        assertEquals("12345", event.getId());
        assertEquals("edit", event.getType());
        assertEquals("enwiki", event.getWiki());
        assertEquals("Test Article", event.getTitle());
        assertEquals("TestUser", event.getUser());
        assertFalse(event.isBot());
        assertEquals("en.wikipedia.org", event.getServerName());
        assertEquals(1700000000L, event.getTimestamp());
        assertEquals(5000, event.getLengthNew());
        assertEquals(4800, event.getLengthOld());
        assertEquals("Fixed typo", event.getComment());
        assertNotNull(event.getMeta());
    }

    @Test
    void testEventDeserializationWithUnknownFields() throws Exception {

        String json = """
                {
                    "id": "99999",
                    "type": "new",
                    "wiki": "dewiki",
                    "title": "New Page",
                    "user": "AnotherUser",
                    "bot": true,
                    "server_name": "de.wikipedia.org",
                    "timestamp": 1700000001,
                    "some_unknown_field": "should be ignored",
                    "another_field": 42
                }
                """;

        WikimediaEvent event = objectMapper.readValue(json, WikimediaEvent.class);

        assertEquals("99999", event.getId());
        assertEquals("new", event.getType());
        assertTrue(event.isBot());
        assertEquals("de.wikipedia.org", event.getServerName());

        assertEquals(0, event.getLengthNew());
        assertEquals(0, event.getLengthOld());
    }

    @Test
    void testEditSizeCalculation() throws Exception {
        String json = """
                {
                    "id": "1",
                    "type": "edit",
                    "wiki": "enwiki",
                    "title": "Test",
                    "user": "User",
                    "bot": false,
                    "server_name": "en.wikipedia.org",
                    "timestamp": 1700000000,
                    "length": { "new": 10000, "old": 8500 }
                }
                """;

        WikimediaEvent event = objectMapper.readValue(json, WikimediaEvent.class);
        int editSize = event.getLengthNew() - event.getLengthOld();

        assertEquals(1500, editSize);
    }
}

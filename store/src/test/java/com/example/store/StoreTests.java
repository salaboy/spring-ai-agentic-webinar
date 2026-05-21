package com.example.store;

import com.example.store.model.Event;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Duration;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(classes = {TestStoreApplication.class, ContainersConfig.class},
        webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
public class StoreTests {

    @Autowired
    KafkaTemplate<String, Event> kafkaTemplate;

    @Autowired
    EventsRestController eventsRestController;

    @Value("${store.events.topic}")
    String topic;

    @BeforeEach
    void setUp() {
        eventsRestController.clearEvents();
    }

    @Test
    void testPubSubWithKafkaTemplate() {
        assertTrue(eventsRestController.getEvents().isEmpty());

        kafkaTemplate.send(topic, new Event("SHIPMENT-123", "shipped", "2024-06-05T12:34:56Z"));

        await().atMost(Duration.ofSeconds(15))
                .until(eventsRestController.getEvents()::size, equalTo(1));
    }
}

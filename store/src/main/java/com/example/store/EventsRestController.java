package com.example.store;

import com.example.store.model.Event;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;

@RestController
@RequestMapping("/api/events")
public class EventsRestController {

    private final EventWebSocketHandler webSocketHandler;
    private final KafkaTemplate<String, Event> kafkaTemplate;
    private final String topic;

    private final List<Event> events = new ArrayList<>();

    public EventsRestController(EventWebSocketHandler webSocketHandler,
                                KafkaTemplate<String, Event> kafkaTemplate,
                                @Value("${store.events.topic}") String topic) {
        this.webSocketHandler = webSocketHandler;
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    public List<Event> getEvents() {
        return events;
    }

    public void clearEvents() {
        events.clear();
    }

    @PostMapping("/mock")
    public void publishMockEvent(@RequestBody Event event) {
        System.out.println(">> Publishing mock event: " + event);
        kafkaTemplate.send(topic, event);
    }

    @KafkaListener(topics = "${store.events.topic}", groupId = "${store.events.group-id}")
    public void onShipmentEvent(Event event) {
        System.out.println(">> Received Kafka event: " + event);
        events.add(event);
        webSocketHandler.broadcastEvent(event);
    }
}

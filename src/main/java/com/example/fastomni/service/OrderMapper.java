package com.example.fastomni.service;

import java.time.Instant;

import com.example.fastomni.kafka.OrderEvent;
import com.example.fastomni.mongo.DeadLetterDocument;
import com.example.fastomni.mongo.OrderDocument;
import org.bson.Document;
import org.springframework.stereotype.Component;


import java.time.Instant;

import org.bson.Document;
import org.springframework.stereotype.Component;

@Component
public class OrderMapper {

    public Document toBsonDocument(
            OrderEvent event,
            String tenantId,
            String correlationId,
            String sourceSystem,
            String eventType
    ) {
        return new Document("_id", event.orderId())
                .append("customerId", event.customerId())
                .append("amount", event.amount())
                .append("createdAt", event.createdAt())
                .append("status", event.status())
                .append("tenantId", tenantId)
                .append("correlationId", correlationId)
                .append("sourceSystem", sourceSystem)
                .append("eventType", eventType)
                .append("receivedAt", Instant.now());
    }

    public Document toDeadLetterBson(DeadLetterDocument dead) {
        return new Document("type", dead.type())
                .append("topic", dead.topic())
                .append("partition", dead.partition())
                .append("offset", dead.offset())
                .append("key", dead.key())
                .append("correlationId", dead.correlationId())
                .append("payload", dead.payload())
                .append("reason", dead.reason())
                .append("receivedAt", dead.receivedAt());
    }

    public DeadLetterDocument deadLetter(
            String type,
            String topic,
            Integer partition,
            Long offset,
            String key,
            String correlationId,
            String payload,
            String reason
    ) {
        return new DeadLetterDocument(
                type,
                topic,
                partition,
                offset,
                key,
                correlationId,
                payload,
                reason,
                Instant.now()
        );
    }
}

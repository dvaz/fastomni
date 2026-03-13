package com.example.fastomni.service;

import com.example.fastomni.kafka.OrderEvent;
import com.example.fastomni.mongo.DeadLetterDocument;
import com.example.fastomni.mongo.OrderDocument;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.bson.Document;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Instant;


@Component
public class OrderMapper {

    public OrderDocument toOrderDocument(OrderEvent event) {
        return new OrderDocument(
                event.orderId(),
                event.customerId(),
                event.amount(),
                event.createdAt(),
                event.status(),
                Instant.now()
        );
    }

    public Document toBsonDocument(OrderDocument order) {
        return new Document("_id", order.id())
                .append("customerId", order.customerId())
                .append("amount", order.amount())
                .append("createdAt", order.createdAt())
                .append("status", order.status())
                .append("receivedAt", order.receivedAt());
    }

    public Document toDeadLetterBson(DeadLetterDocument dead) {
        return new Document("type", dead.type())
                .append("topic", dead.topic())
                .append("partition", dead.partition())
                .append("offset", dead.offset())
                .append("key", dead.key())
                .append("payload", dead.payload())
                .append("reason", dead.reason())
                .append("receivedAt", dead.receivedAt());
    }

    public DeadLetterDocument deserializationError(ConsumerRecord<String, OrderEvent> record, String payload, String reason) {
        return new DeadLetterDocument(
                "DESERIALIZATION_ERROR",
                record.topic(),
                record.partition(),
                record.offset(),
                record.key(),
                payload,
                reason,
                Instant.now()
        );
    }

    public DeadLetterDocument duplicateKey(ConsumerRecord<String, OrderEvent> record, String payload, String reason) {
        return new DeadLetterDocument(
                "DUPLICATE_KEY",
                record.topic(),
                record.partition(),
                record.offset(),
                record.key(),
                payload,
                reason,
                Instant.now()
        );
    }

    public DeadLetterDocument insertError(ConsumerRecord<String, OrderEvent> record, String payload, String reason) {
        return new DeadLetterDocument(
                "INSERT_ERROR",
                record.topic(),
                record.partition(),
                record.offset(),
                record.key(),
                payload,
                reason,
                Instant.now()
        );
    }

    public String payloadAsString(byte[] raw) {
        if (raw == null) {
            return null;
        }
        return new String(raw, StandardCharsets.UTF_8);
    }
}

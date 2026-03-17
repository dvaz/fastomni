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

    public OrderDocument toOrderDocument(
            OrderEvent event,
            String tenantId,
            String correlationId,
            String sourceSystem
    ) {
        return new OrderDocument(
                event.orderId(),
                event.customerId(),
                event.amount(),
                event.createdAt(),
                event.status(),
                tenantId,
                correlationId,
                sourceSystem,
                Instant.now()
        );
    }

    public Document toBsonDocument(OrderDocument order) {
        return new Document("_id", order.id())
                .append("customerId", order.customerId())
                .append("amount", order.amount())
                .append("createdAt", order.createdAt())
                .append("status", order.status())
                .append("tenantId", order.tenantId())
                .append("correlationId", order.correlationId())
                .append("sourceSystem", order.sourceSystem())
                .append("receivedAt", order.receivedAt());
    }

    public DeadLetterDocument deserializationError(
            String topic,
            Integer partition,
            Long offset,
            String key,
            String correlationId,
            String payload,
            String reason
    ) {
        return new DeadLetterDocument(
                "DESERIALIZATION_ERROR",
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

    public DeadLetterDocument validationError(
            String topic,
            Integer partition,
            Long offset,
            String key,
            String correlationId,
            String payload,
            String reason
    ) {
        return new DeadLetterDocument(
                "VALIDATION_ERROR",
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

    public DeadLetterDocument duplicateKey(
            String topic,
            Integer partition,
            Long offset,
            String key,
            String correlationId,
            String payload,
            String reason
    ) {
        return new DeadLetterDocument(
                "DUPLICATE_KEY",
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

    public DeadLetterDocument insertError(
            String topic,
            Integer partition,
            Long offset,
            String key,
            String correlationId,
            String payload,
            String reason
    ) {
        return new DeadLetterDocument(
                "INSERT_ERROR",
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
}


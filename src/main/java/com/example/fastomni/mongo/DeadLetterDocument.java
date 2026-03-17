package com.example.fastomni.mongo;

import java.time.Instant;

public
    record DeadLetterDocument(
            String type,              // DESERIALIZATION_ERROR | DUPLICATE_KEY | INSERT_ERROR
            String topic,
            Integer partition,
            Long offset,
            String key,
            String correlationId,
            String payload,
            String reason,
            Instant receivedAt
    ) {
}

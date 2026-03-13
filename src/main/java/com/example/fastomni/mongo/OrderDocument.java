package com.example.fastomni.mongo;

import java.math.BigDecimal;
import java.time.Instant;

public record OrderDocument(String id,
                            String customerId,
                            BigDecimal amount,
                            Instant createdAt,
                            String status,
                            Instant receivedAt) {
}

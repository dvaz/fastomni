package com.example.fastomni.kafka;

import java.math.BigDecimal;
import java.time.Instant;

public record OrderEvent(String orderId,
                         String customerId,
                         BigDecimal amount,
                         Instant createdAt,
                         String status) {
}

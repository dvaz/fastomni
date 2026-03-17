package com.example.fastomni.api;


import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import com.example.fastomni.kafka.OrderEvent;
import com.example.fastomni.kafka.OrderProducer;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.*;


@RestController
@RequestMapping("/orders")
public class OrderCommandController {

    private final OrderProducer orderProducer;

    public OrderCommandController(OrderProducer orderProducer) {
        this.orderProducer = orderProducer;
    }

    @PostMapping("/publish")
    public CompletableFuture<ResponseEntity<String>> publish(@RequestBody PublishOrderRequest request) {
        String correlationId = UUID.randomUUID().toString();

        OrderEvent event = new OrderEvent(
                request.orderId(),
                request.customerId(),
                request.amount(),
                Instant.now(),
                request.status()
        );

        return orderProducer.send(
                event,
                "topic-name",
                request.tenantId(),
                "order-command-api"
        ).thenApply(result -> ResponseEntity.ok(
                "Evento enviado com sucesso. topic=%s partition=%d offset=%d correlationId=%s"
                        .formatted(
                                result.getRecordMetadata().topic(),
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset(),
                                correlationId
                        )
        ));
    }

    @PostMapping("/publish-fire-and-forget")
    public ResponseEntity<String> publishFireAndForget(@RequestBody PublishOrderRequest request) {
        String correlationId = UUID.randomUUID().toString();

        OrderEvent event = new OrderEvent(
                request.orderId(),
                request.customerId(),
                request.amount(),
                Instant.now(),
                request.status()
        );

        orderProducer.sendWithCallback(
                event,
                "topic",
                request.tenantId(),
                correlationId,
                "order-command-api"
        );

        return ResponseEntity.accepted()
                .body("Envio iniciado. correlationId=" + correlationId);
    }

    public record PublishOrderRequest(
            String orderId,
            String customerId,
            BigDecimal amount,
            String status,
            String tenantId
    ) {
    }
}
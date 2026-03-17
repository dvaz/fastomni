package com.example.fastomni.config;

import com.example.fastomni.kafka.OrderEvent;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaConsumerConfig {

    @Bean
    ConcurrentKafkaListenerContainerFactory<String, OrderEvent> batchKafkaListenerContainerFactory(
            ConsumerFactory<String, OrderEvent> consumerFactory,
            @Value("${app.consumer.bulk.concurrency}") int concurrency
    ) {
        ConcurrentKafkaListenerContainerFactory<String, OrderEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory);
        factory.setBatchListener(true);
        factory.setConcurrency(concurrency);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        factory.getContainerProperties().setPollTimeout(1500);
        factory.setCommonErrorHandler(commonErrorHandler());

        return factory;
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<String, OrderEvent> singleKafkaListenerContainerFactory(
            ConsumerFactory<String, OrderEvent> consumerFactory,
            @Value("${app.consumer.single.concurrency}") int concurrency
    ) {
        ConcurrentKafkaListenerContainerFactory<String, OrderEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory);
        factory.setBatchListener(false);
        factory.setConcurrency(concurrency);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        factory.getContainerProperties().setPollTimeout(1500);
        factory.setCommonErrorHandler(commonErrorHandler());

        return factory;
    }

    @Bean
    CommonErrorHandler commonErrorHandler() {
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
                new FixedBackOff(1000L, 2L)
        );
        errorHandler.addNotRetryableExceptions(
                IllegalArgumentException.class,
                ClassCastException.class
        );
        return errorHandler;
    }
}
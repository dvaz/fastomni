package com.example.fastomni.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;
/**
 * Classe de configuração para o modelo de controle dos erros quando fizer
 * o processamento de LOTE para aceitar parcialmente os que tiveram sucesso
 */
@Configuration
public class KafkaConsumerControlConfig {
    @Bean(name = "kafkaListenerContainerFactoryByControl")
    ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactoryByControl(
            ConsumerFactory<String, Object> consumerFactory,
            @Value("${spring.kafka.listener.concurrency:6}") int concurrency,
            @Value("${spring.kafka.listener.poll-timeout:1500}") long pollTimeout
    ) {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory);
        factory.setBatchListener(true);
        factory.setConcurrency(concurrency);
        factory.getContainerProperties().setPollTimeout(pollTimeout);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        factory.setCommonErrorHandler(commonErrorHandlerByControl());

        return factory;
    }

    @Bean(name = "commonErrorHandlerByControl")
    CommonErrorHandler commonErrorHandlerByControl() {
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(new FixedBackOff(1000L, 2L));
        errorHandler.addNotRetryableExceptions(
                IllegalArgumentException.class,
                ClassCastException.class
        );
        return errorHandler;
    }
}

package com.example.fastomni.config;

import java.util.List;
import java.util.concurrent.TimeUnit;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCompressor;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


/**
 * Aqui o ganho importante está em:
 *
 * pool maior
 * maxConnecting
 * retryWrites
 * Snappy compressor
 * WriteConcern.W1
 * insertMany nativo depois
 */
@Configuration
public class MongoClientConfig {

    @Bean
    MongoClient mongoClient(
            @Value("${spring.data.mongodb.uri}") String uri,
            @Value("${app.mongo.max-pool-size}") int maxPoolSize,
            @Value("${app.mongo.min-pool-size}") int minPoolSize,
            @Value("${app.mongo.max-connecting}") int maxConnecting,
            @Value("${app.mongo.connect-timeout-ms}") int connectTimeoutMs,
            @Value("${app.mongo.read-timeout-ms}") int readTimeoutMs,
            @Value("${app.mongo.server-selection-timeout-ms}") int serverSelectionTimeoutMs
    ) {
        ConnectionString connectionString = new ConnectionString(uri);

        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(connectionString)
                .retryWrites(true)
                .writeConcern(WriteConcern.W1)
                .readPreference(ReadPreference.primary())
                .compressorList(List.of(
                        MongoCompressor.createSnappyCompressor()
                ))
                .applyToConnectionPoolSettings(pool -> pool
                        .maxSize(maxPoolSize)
                        .minSize(minPoolSize)
                        .maxConnecting(maxConnecting)
                        .maxWaitTime(5, TimeUnit.SECONDS)
                        .maxConnectionIdleTime(60, TimeUnit.SECONDS)
                        .maxConnectionLifeTime(30, TimeUnit.MINUTES))
                .applyToSocketSettings(socket -> socket
                        .connectTimeout(connectTimeoutMs, TimeUnit.MILLISECONDS)
                        .readTimeout(readTimeoutMs, TimeUnit.MILLISECONDS))
                .applyToClusterSettings(cluster -> cluster
                        .serverSelectionTimeout(serverSelectionTimeoutMs, TimeUnit.MILLISECONDS))
                .build();

        return MongoClients.create(settings);
    }
}
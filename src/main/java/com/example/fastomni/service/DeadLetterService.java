package com.example.fastomni.service;

import java.util.List;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.InsertManyOptions;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class DeadLetterService {

    private final MongoCollection<Document> deadLetterCollection;

    public DeadLetterService(
            MongoClient mongoClient,
            @Value("${app.mongo.database}") String database,
            @Value("${app.mongo.dead-letter-collection}") String collectionName
    ) {
        this.deadLetterCollection = mongoClient
                .getDatabase(database)
                .getCollection(collectionName);
    }

    public void saveAll(List<Document> deadLetters) {
        if (deadLetters == null || deadLetters.isEmpty()) {
            return;
        }
        deadLetterCollection.insertMany(deadLetters, new InsertManyOptions().ordered(false));
    }
}
package com.example.fastomni.service;

import com.mongodb.MongoWriteException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.mongodb.MongoWriteException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class MongoSimpleInsertService {

    private final MongoCollection<Document> ordersCollection;

    public MongoSimpleInsertService(
            MongoClient mongoClient,
            @Value("${app.mongo.database}") String database,
            @Value("${app.mongo.collection}") String collectionName
    ) {
        this.ordersCollection = mongoClient
                .getDatabase(database)
                .getCollection(collectionName);
    }

    public InsertResult insertOne(Document doc) {

        try {
            ordersCollection.insertOne(doc);
            return InsertResult.INSERTED;

        } catch (MongoWriteException e) {

            int code = e.getError() != null ? e.getError().getCode() : -1;

            if (code == 11000 || code == 11001) {
                return InsertResult.DUPLICATE;
            }

            throw e;
        }
    }

    public enum InsertResult {
        INSERTED,
        DUPLICATE
    }
}
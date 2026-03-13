package com.example.fastomni.service;


import java.util.ArrayList;
import java.util.List;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.InsertManyOptions;

import org.bson.Document;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
/**
 * Aqui está a principal otimização: usar o driver Mongo nativo com insertMany.
 *
 * Como teu cenário é somente insert, eu prefiro isso ao MongoTemplate.bulkOps(...) para este caso específico.
 */

@Service
public class MongoOrderInsertService {

    private final MongoCollection<Document> collection;

    public MongoOrderInsertService(
            MongoClient mongoClient,
            @Value("${app.mongo.database}") String database,
            @Value("${app.mongo.collection}") String collectionName
    ) {
        this.collection = mongoClient
                .getDatabase(database)
                .getCollection(collectionName);
    }

    public int insertManyUnordered(List<Document> docs) {
        if (docs == null || docs.isEmpty()) {
            return 0;
        }

        collection.insertMany(
                docs,
                new InsertManyOptions().ordered(false)
        );

        return docs.size();
    }
}
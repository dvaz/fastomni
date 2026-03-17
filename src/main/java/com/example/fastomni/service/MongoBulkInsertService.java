package com.example.fastomni.service;

import java.util.List;
import java.util.Set;
import java.util.HashSet;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.InsertManyOptions;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 *insertMany(..., ordered(false)) é o caminho mais forte para insert puro em lote quando
 * você quer throughput e quer que documentos válidos sigam mesmo se alguns falharem.
 */
@Service
public class MongoBulkInsertService {

    private final MongoCollection<Document> ordersCollection;

    public MongoBulkInsertService(
            MongoClient mongoClient,
            @Value("${app.mongo.database}") String database,
            @Value("${app.mongo.collection}") String collectionName
    ) {
        this.ordersCollection = mongoClient
                .getDatabase(database)
                .getCollection(collectionName);
    }

    public InsertOutcome insertManyUnordered(List<Document> docs) {
        if (docs == null || docs.isEmpty()) {
            return InsertOutcome.empty();
        }

        try {
            ordersCollection.insertMany(docs, new InsertManyOptions().ordered(false));
            return InsertOutcome.success(docs.size());
        } catch (MongoBulkWriteException e) {
            Set<Integer> duplicateIndexes = new HashSet<>();
            Set<Integer> failedIndexes = new HashSet<>();

            for (BulkWriteError error : e.getWriteErrors()) {
                failedIndexes.add(error.getIndex());
                if (error.getCode() == 11000 || error.getCode() == 11001) {
                    duplicateIndexes.add(error.getIndex());
                }
            }

            int inserted = docs.size() - failedIndexes.size();
            return new InsertOutcome(inserted, duplicateIndexes, failedIndexes);
        }
    }

    public record InsertOutcome(
            int insertedCount,
            Set<Integer> duplicateIndexes,
            Set<Integer> failedIndexes
    ) {
        public static InsertOutcome empty() {
            return new InsertOutcome(0, Set.of(), Set.of());
        }

        public static InsertOutcome success(int insertedCount) {
            return new InsertOutcome(insertedCount, Set.of(), Set.of());
        }
    }
}
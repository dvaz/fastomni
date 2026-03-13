package com.example.fastomni.service;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.InsertManyOptions;

import org.bson.Document;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 * classe com controle de insert e erros dentro do lot, para permitir aceitar saves parciais e expor quais foram os que deram errado
 * O que esse serviço faz
 * se tudo der certo, retorna sucesso total
 * se houver duplicado, ele não derruba os válidos
 * ele te devolve exatamente os índices duplicados e os índices que falharam no lote, o que é compatível com o comportamento de insertMany(... ordered(false)).
 */
public class MongoOrderInsertControlService {

    private final MongoCollection<Document> ordersCollection;
    private final MongoCollection<Document> deadLetterCollection;

    public MongoOrderInsertControlService(
            MongoClient mongoClient,
            @Value("${app.mongo.database}") String database,
            @Value("${app.mongo.collection}") String ordersCollectionName,
            @Value("${app.mongo.dead-letter-collection}") String deadLetterCollectionName
    ) {
        this.ordersCollection = mongoClient
                .getDatabase(database)
                .getCollection(ordersCollectionName);

        this.deadLetterCollection = mongoClient
                .getDatabase(database)
                .getCollection(deadLetterCollectionName);
    }

    public InsertOutcome insertOrdersUnordered(List<Document> docs) {
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

            return new InsertOutcome(
                    inserted,
                    duplicateIndexes,
                    failedIndexes,
                    e.getWriteErrors().stream().map(BulkWriteError::getMessage).toList()
            );
        }
    }

    public void saveDeadLetters(List<Document> deadLetters) {
        if (deadLetters == null || deadLetters.isEmpty()) {
            return;
        }
        deadLetterCollection.insertMany(deadLetters, new InsertManyOptions().ordered(false));
    }

    public record InsertOutcome(
            int insertedCount,
            Set<Integer> duplicateIndexes,
            Set<Integer> failedIndexes,
            List<String> errors
    ) {
        public static InsertOutcome empty() {
            return new InsertOutcome(0, Set.of(), Set.of(), List.of());
        }

        public static InsertOutcome success(int insertedCount) {
            return new InsertOutcome(insertedCount, Set.of(), Set.of(), List.of());
        }
    }
}

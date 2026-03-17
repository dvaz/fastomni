# Getting Started

### Reference Documentation
For further reference, please consider the following sections:

* [Official Gradle documentation](https://docs.gradle.org)
* [Spring Boot Gradle Plugin Reference Guide](https://docs.spring.io/spring-boot/3.5.11/gradle-plugin)
* [Create an OCI image](https://docs.spring.io/spring-boot/3.5.11/gradle-plugin/packaging-oci-image.html)

### Additional Links
These additional references should also help you:

* [Gradle Build Scans – insights for your project's build](https://scans.gradle.com#gradle)

### Os ganhos mais relevantes aqui são:

### 1. insertMany direto
* Para insert puro, você evita parte do overhead de abstrações extras e usa a API nativa de inserção múltipla do driver. O driver Mongo distingue insertOne, insertMany e bulkWrite; insertMany é apropriado quando a operação é só inserir.

### 2. ordered(false)
* Isso evita que uma falha em um documento interrompa toda a sequência no mesmo estilo estritamente ordenado. Para throughput, costuma ajudar bastante. O MongoDB documenta o comportamento de insertMany e escrita múltipla.

### 3. mapeamento mais direto
* Em vez de passar tudo pelo MongoTemplate e conversão extra, o código já monta org.bson.Document.

### 4. tuning de fetch do Kafka
* fetch.min.bytes, fetch.max.wait.ms, max.partition.fetch.bytes, max.poll.records ajudam a fazer o consumidor trabalhar com batches mais cheios quando o tópico está muito carregado.

### 5. ack só no final
* No Spring Kafka, o uso de Acknowledgment depende do modo manual, e isso mantém o offset sob teu controle.

## Sobre virtual threads

Eu deixei:

``spring.threads.virtual.enabled=true``

O Spring Boot suporta isso em Java 21+, mas o próprio Boot alerta que pode haver perda de throughput em casos de pinned virtual threads. Então eu trataria isso como teste controlado, não como ganho garantido.

### Nesse desenho, o ganho principal vem de:
* batch Kafka
* insertMany
* unordered
* chunking
* pool do Mongo
* concorrência controlada

As virtual threads são um extra experimental.

### Ajustes que eu testaria primeiro em produção

Começaria assim:
```  
spring.kafka.listener.concurrency=6
spring.kafka.consumer.max-poll-records=1000
app.insert.batch-size=1000
app.insert.parallelism=8
app.mongo.max-pool-size=120
```

### Depois faria benchmark variando só um eixo por vez:

* concurrency: 4 / 6 / 8
* max-poll-records: 500 / 1000 / 2000
* batch-size: 500 / 1000 / 1500
* parallelism: 6 / 8 / 12

### pontos de atenção  - Cuidados importantes
#### Duplicidade

Como você disse que não quer upsert, tudo bem. Mas se houver redelivery do Kafka antes do ack, pode entrar duplicado. O ideal é usar _id = orderId, como no código.

#### Falha parcial

Com ordered(false), uma falha de chave duplicada ou documento inválido pode resultar em parte do lote sendo gravada. Nesse caso, teu tratamento de erro deve ser decidido com cuidado.

#### Índices

Se a coleção tiver muitos índices, o insert vai piorar bastante.

#### Batch deserialization

No Spring Kafka, em batch listener, erro de deserialização precisa ser tratado com mais atenção; por isso o ErrorHandlingDeserializer é importante.



## modelos que tem CONTROL no nome segue essa premissa

Como isso se comporta
##  Caso 1: 1000 mensagens, 10 duplicadas
* 990 entram no Mongo
* 10 vão para a collection dead-letter
* o batch é ackado
* nada trava
* Isso é coerente com o comportamento de ordered(false) no Mongo.

## Caso 2: 1000 mensagens, 1 JSON inválido

* 999 seguem
* 1 vai para dead-letter como DESERIALIZATION_ERROR
* o batch é ackado
* você não perde as válidas
* Isso exige inspeção manual dos headers com ErrorHandlingDeserializer em batch listener.

## Caso 3: erro real de banco não relacionado a duplicidade
* registra a falha
* lança exceção
* não dá ack
* Kafka reprocessa conforme a política do teu container/error handler

## Melhor ajuste para deserialização por item

O código acima usa uma checagem simples por header. O ideal é enriquecer isso usando os utilitários do Spring Kafka para recuperar a exceção do header e, se necessário, os bytes brutos do payload. A documentação do Spring Kafka diz que, com ErrorHandlingDeserializer, a DeserializationException fica disponível para o handler/listener e contém os dados brutos.

Na prática, você pode evoluir para algo assim:
```
Header header = record.headers().lastHeader("springDeserializerExceptionValue");
if (header != null) {
    // desserializar o header para obter detalhes da DeserializationException
    // e registrar bytes brutos do payload, se necessário
}
```


src/main/java/com/example/kafkamongo
├─ KafkaMongoApplication.java
├─ config
│  ├─ KafkaConsumerConfig.java
│  ├─ KafkaProducerConfig.java
│  └─ MongoClientConfig.java
├─ kafka
│  └─ OrderEvent.java
├─ mongo
│  ├─ DeadLetterDocument.java
│  └─ OrderDocument.java
├─ producer
│  └─ OrderProducer.java
├─ service
│  ├─ DeadLetterService.java
│  ├─ MongoBulkInsertService.java
│  ├─ MongoSimpleInsertService.java
│  └─ OrderMapper.java
└─ consumer
├─ BulkOrderKafkaListener.java
└─ SimpleOrderKafkaListener.java
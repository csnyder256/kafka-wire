// Produce and consume against kafka-wire with the Apache Kafka Java client.
//
//   cd examples/java && mvn --batch-mode compile exec:java
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.common.utils.AppInfoParser;
import java.util.concurrent.TimeUnit;

public class Roundtrip {
    static final String TOPIC = "demo.java." + UUID.randomUUID().toString().replace("-", "");

    public static void main(String[] args) throws Exception {
        String brokers = System.getenv().getOrDefault("KAFKA_WIRE_BROKERS", "127.0.0.1:9092");

        Properties common = new Properties();
        common.put("bootstrap.servers", brokers);
        common.put("request.timeout.ms", "15000");
        common.put("default.api.timeout.ms", "30000");

        try (Admin admin = Admin.create(common)) {
            admin.createTopics(List.of(new NewTopic(TOPIC, 1, (short) 1))).all().get(30, TimeUnit.SECONDS);
        }

        Properties producerProps = new Properties();
        producerProps.putAll(common);
        producerProps.put("key.serializer", ByteArraySerializer.class.getName());
        producerProps.put("value.serializer", ByteArraySerializer.class.getName());
        // REQUIRED. Since Kafka 3.0 this defaults to true, which makes the
        // producer demand InitProducerId. kafka-wire has no transaction
        // coordinator and does not offer that API, and the Java client treats
        // its absence as fatal rather than falling back.
        producerProps.put("enable.idempotence", "false");
        producerProps.put("max.block.ms", "30000");
        producerProps.put("delivery.timeout.ms", "30000");
        producerProps.put("acks", "all");

        byte[] everyByte = new byte[256];
        for (int i = 0; i < 256; i++) everyByte[i] = (byte) i;

        List<byte[]> messages = List.of(
            "a plain line".getBytes(StandardCharsets.UTF_8),
            "{\"id\":1,\"note\":\"json is just bytes here\"}".getBytes(StandardCharsets.UTF_8),
            everyByte,
            new byte[0],
            "こんにちは · Kafka".getBytes(StandardCharsets.UTF_8)
        );

        try (Producer<byte[], byte[]> producer = new KafkaProducer<>(producerProps)) {
            for (byte[] m : messages) {
                producer.send(new ProducerRecord<>(TOPIC, "k".getBytes(StandardCharsets.UTF_8), m)).get(30, TimeUnit.SECONDS);
            }
        }
        System.out.printf("produced %d records to %s%n", messages.size(), TOPIC);

        Properties consumerProps = new Properties();
        consumerProps.putAll(common);
        consumerProps.put("key.deserializer", ByteArrayDeserializer.class.getName());
        consumerProps.put("value.deserializer", ByteArrayDeserializer.class.getName());
        consumerProps.put("group.id", TOPIC + ".group");
        consumerProps.put("auto.offset.reset", "earliest");
        consumerProps.put("group.protocol", "classic");
        consumerProps.put("enable.auto.commit", "false");

        List<byte[]> received = new ArrayList<>();
        try (Consumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(List.of(TOPIC));
            long deadline = System.currentTimeMillis() + 15_000;
            while (received.size() < messages.size() && System.currentTimeMillis() < deadline) {
                for (ConsumerRecord<byte[], byte[]> r : consumer.poll(Duration.ofSeconds(2))) {
                    if(r.partition()!=0||r.offset()!=received.size()||!Arrays.equals(r.key(), "k".getBytes(StandardCharsets.UTF_8)))throw new IllegalStateException("key/order mismatch");
                    received.add(r.value());
                }
            }
            if(received.size()==messages.size()){
                consumer.commitSync(Duration.ofSeconds(15));
                var committed=consumer.committed(Set.of(new org.apache.kafka.common.TopicPartition(TOPIC,0)),Duration.ofSeconds(15));
                if(committed.size()!=1||committed.values().stream().anyMatch(v->v==null||v.offset()!=messages.size()))throw new IllegalStateException("offset mismatch");
            }
        }

        boolean ok = received.size() == messages.size();
        for (int i = 0; ok && i < received.size(); i++) {
            ok = Arrays.equals(received.get(i), messages.get(i));
        }
        System.out.println(ok
            ? String.format("consumed %d records, byte-identical to what was sent", received.size())
            : String.format("MISMATCH: sent %d, got %d", messages.size(), received.size()));
        if (!ok) System.exit(1);
        System.out.printf("{\"schema\":\"kafka-wire.client-check\",\"version\":1,\"client\":\"Apache Kafka\",\"client_version\":\"%s\",\"language\":\"java\",\"runtime\":\"%s\",\"status\":\"passed\",\"records\":%d,\"checks\":[\"create-topic\",\"produce-acks\",\"byte-fidelity\",\"key-fidelity\",\"partition-order\",\"classic-group\",\"offset-commit-fetch\"],\"settings\":{\"idempotence\":false,\"compression\":\"none\",\"partitions\":1,\"security\":\"PLAINTEXT\"}}%n",AppInfoParser.getVersion(),System.getProperty("java.version"),received.size());
    }
}

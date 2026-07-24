// Produce and consume against kafka-wire with the Apache Kafka Java client.
//
//   javac -cp kafka-clients-3.9.0.jar Roundtrip.java
//   java  -cp .:kafka-clients-3.9.0.jar:slf4j-api-2.0.13.jar Roundtrip
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;

public class Roundtrip {
    static final String TOPIC = "demo.java";

    public static void main(String[] args) throws Exception {
        String brokers = System.getenv().getOrDefault("KAFKA_WIRE_BROKERS", "127.0.0.1:9092");

        Properties common = new Properties();
        common.put("bootstrap.servers", brokers);

        try (Admin admin = Admin.create(common)) {
            admin.createTopics(List.of(new NewTopic(TOPIC, 1, (short) 1))).all().get();
        } catch (Exception ignored) {
            // Already exists.
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

        byte[] everyByte = new byte[256];
        for (int i = 0; i < 256; i++) everyByte[i] = (byte) i;

        List<byte[]> messages = List.of(
            "a plain line".getBytes(StandardCharsets.UTF_8),
            "{\"id\":1,\"note\":\"json is just bytes here\"}".getBytes(StandardCharsets.UTF_8),
            everyByte,
            new byte[0]
        );

        try (Producer<byte[], byte[]> producer = new KafkaProducer<>(producerProps)) {
            for (byte[] m : messages) {
                producer.send(new ProducerRecord<>(TOPIC, "k".getBytes(StandardCharsets.UTF_8), m)).get();
            }
        }
        System.out.printf("produced %d records to %s%n", messages.size(), TOPIC);

        Properties consumerProps = new Properties();
        consumerProps.putAll(common);
        consumerProps.put("key.deserializer", ByteArrayDeserializer.class.getName());
        consumerProps.put("value.deserializer", ByteArrayDeserializer.class.getName());
        consumerProps.put("group.id", "demo-java-group");
        consumerProps.put("auto.offset.reset", "earliest");

        List<byte[]> received = new ArrayList<>();
        try (Consumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(List.of(TOPIC));
            long deadline = System.currentTimeMillis() + 15_000;
            while (received.size() < messages.size() && System.currentTimeMillis() < deadline) {
                for (ConsumerRecord<byte[], byte[]> r : consumer.poll(Duration.ofSeconds(2))) {
                    received.add(r.value());
                }
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
    }
}

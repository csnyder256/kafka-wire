// Produce and consume against kafka-wire with KafkaJS.
//
//   npm install kafkajs
//   node roundtrip.mjs
import { Kafka, logLevel } from "kafkajs";

const brokers = (process.env.KAFKA_WIRE_BROKERS ?? "127.0.0.1:9092").split(",");
const topic = "demo.nodejs";

const kafka = new Kafka({ clientId: "kafka-wire-example", brokers, logLevel: logLevel.ERROR });

const messages = [
  Buffer.from("a plain line"),
  Buffer.from(JSON.stringify({ id: 1, note: "json is just bytes here" })),
  Buffer.from(Array.from({ length: 256 }, (_, i) => i)), // every byte value
  Buffer.alloc(0),                                       // empty, not absent
];

const admin = kafka.admin();
await admin.connect();
await admin.createTopics({ topics: [{ topic, numPartitions: 1 }] });
await admin.disconnect();

const producer = kafka.producer();
await producer.connect();
await producer.send({ topic, messages: messages.map((value) => ({ key: "k", value })) });
await producer.disconnect();
console.log(`produced ${messages.length} records to ${topic}`);

const consumer = kafka.consumer({ groupId: "demo-nodejs-group" });
await consumer.connect();
await consumer.subscribe({ topic, fromBeginning: true });

const received = [];
await consumer.run({
  eachMessage: async ({ message }) => {
    received.push(message.value ?? Buffer.alloc(0));
  },
});

// Give the fetch loop a moment, then compare byte for byte.
await new Promise((r) => setTimeout(r, 5000));
await consumer.disconnect();

const ok =
  received.length === messages.length &&
  received.every((buf, i) => Buffer.compare(buf, messages[i]) === 0);

console.log(
  ok
    ? `consumed ${received.length} records, byte-identical to what was sent`
    : `MISMATCH: sent ${messages.length}, got ${received.length}`,
);
process.exit(ok ? 0 : 1);

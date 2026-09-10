// Central Kafka client for VarshaNet's streaming ingestion layer.
//
// This is the one place that knows how to reach the broker — every
// producer (the ingestion pipelines in server/ingest.js, sachetIngest.js,
// imdCapIngest.js, plus the citizen report route) calls getProducer() from
// here instead of building its own kafkajs Kafka() instance, and the
// consumer (server/worker.js) calls createConsumer().
//
// Works against any Kafka-API-compatible broker: a local single-node
// Redpanda container (see docker-compose.yml — no SASL needed for that),
// Redpanda Cloud/Serverless, Confluent Cloud, or a self-hosted Apache Kafka
// cluster — only the env vars below change; no code here does.

const { Kafka, logLevel } = require("kafkajs");

const BROKERS = (process.env.KAFKA_BROKERS || "localhost:19092")
  .split(",")
  .map((b) => b.trim())
  .filter(Boolean);

const SSL_ENABLED = (process.env.KAFKA_SSL || "false").toLowerCase() === "true";

// SASL is optional — the local dev broker in docker-compose.yml runs with
// no auth at all, so only build the `sasl` block when credentials are
// actually set (a managed cloud broker like Redpanda Serverless will need
// this; check its console for the exact mechanism it expects).
const SASL =
  process.env.KAFKA_USERNAME && process.env.KAFKA_PASSWORD
    ? {
        mechanism: process.env.KAFKA_SASL_MECHANISM || "scram-sha-256",
        username: process.env.KAFKA_USERNAME,
        password: process.env.KAFKA_PASSWORD,
      }
    : undefined;

const kafka = new Kafka({
  clientId: "varshanet",
  brokers: BROKERS,
  ssl: SSL_ENABLED,
  sasl: SASL,
  logLevel: logLevel.NOTHING, // kafkajs is chatty by default; this app logs its own producer/consumer lifecycle events instead
  retry: { retries: 5 },
});

let producer = null;
let producerConnectPromise = null;

// Lazily connects one shared producer the first time anything tries to
// publish, then reuses that same connection for every later publish — the
// same "connect once, reuse the handle" pattern server/db.js already uses
// for its MongoClient.
async function getProducer() {
  if (producer) return producer;
  if (producerConnectPromise) return producerConnectPromise;

  producerConnectPromise = (async () => {
    const p = kafka.producer({ allowAutoTopicCreation: true });
    await p.connect();
    console.log(`Kafka producer connected (brokers: ${BROKERS.join(", ")})`);
    producer = p;
    return producer;
  })();

  return producerConnectPromise;
}

// Each caller gets its own consumer bound to `groupId`. kafkajs consumer
// groups are how VarshaNet gets horizontal scaling for free: run
// server/worker.js as two, three, or ten separate processes with the same
// groupId (same KAFKA_CONSUMER_GROUP env var) and Kafka automatically
// splits the topic's partitions across them — no extra code needed for
// that, it's a property of the protocol.
function createConsumer(groupId) {
  return kafka.consumer({ groupId, allowAutoTopicCreation: true });
}

async function disconnectProducer() {
  if (producer) {
    await producer.disconnect();
    producer = null;
    producerConnectPromise = null;
  }
}

module.exports = { kafka, getProducer, createConsumer, disconnectProducer };

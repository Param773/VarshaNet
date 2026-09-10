// Shared "publish a candidate report onto the stream" helper used by all
// five automated ingestion pipelines: ingest.js, sachetIngest.js,
// imdCapIngest.js, blueskyIngest.js, mastodonIngest.js.
//
// Before this existed, each pipeline called scoreReport() + db.addReport()
// directly, inline — five separate places doing the exact same "score,
// then write to Mongo" step with five separate chances to drift apart.
// Now each pipeline's job ends the moment it has found a candidate report:
// it publishes the raw fields onto the varshanet.raw-reports Kafka topic
// (see topics.js) and moves on immediately, instead of blocking on a
// scoring computation and a database round-trip per item.
// server/worker.js's consumer group is the ONLY place scoreReport() /
// db.addReport() run for auto-ingested data now — one implementation
// instead of five, and a pipeline that publishes 50 candidates in one run
// no longer pays for 50 sequential Mongo writes before it can move on to
// its next poll.
//
// Partitioned by city: every event for the same city lands on the same
// partition, so any single consumer instance always sees that city's
// events in the order they were published.

const { getProducer } = require("./kafka");
const { RAW_REPORTS } = require("./topics");

async function publishRawReport(payload) {
  const producer = await getProducer();
  await producer.send({
    topic: RAW_REPORTS,
    messages: [
      {
        key: payload.city || "unknown",
        value: JSON.stringify({ ...payload, publishedAt: Date.now() }),
      },
    ],
  });
  return { queued: true, ...payload };
}

module.exports = { publishRawReport };

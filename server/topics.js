// Topic names in one place, so a rename or a new environment's topic
// prefix never means grepping through five ingestion files.

module.exports = {
  // Raw, unscored candidate reports from the 5 automated ingestion
  // pipelines. server/worker.js's consumer group is the only reader —
  // scoring + the MongoDB write both happen there.
  RAW_REPORTS: process.env.KAFKA_TOPIC_RAW_REPORTS || "varshanet.raw-reports",

  // A message that failed to score/persist lands here instead of being
  // silently dropped, so it can be inspected or replayed later.
  RAW_REPORTS_DLQ: process.env.KAFKA_TOPIC_DLQ || "varshanet.raw-reports.dlq",

  // Fire-and-forget event emitted after every synchronous citizen-report
  // submission (server/routes/reports.js) — not consumed by the scoring
  // worker (those reports are already scored + saved by the time this is
  // published), but keeps a unified stream of "something happened" events
  // available for a future live/audit consumer.
  ACTIVITY: process.env.KAFKA_TOPIC_ACTIVITY || "varshanet.activity",
};

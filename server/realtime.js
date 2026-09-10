// Bridges the Kafka `ACTIVITY` topic (server/topics.js) to WebSocket
// clients, so the dashboard's "Live Dashboard" is actually live — a report
// being created or changing status reaches every open browser tab within
// about a second of the Kafka publish, instead of waiting up to 25s for
// the next poll (see public/index.html's old setInterval-based
// `loadReports` loop, now a fallback rather than the primary path — see
// its comment there).
//
// Every write path that touches a report already publishes to ACTIVITY:
//   - server/worker.js        — auto-ingested reports, right after scoring
//   - server/routes/reports.js — citizen submissions + admin/moderator
//                                 approve/reject
//   - server/autoResolve.js   — corroboration/timeout resolutions
// This file is the one place that turns that Kafka stream into a browser
// push; nothing above needs to know WebSocket exists.
//
// Deliberately its own dedicated consumer group ("varshanet-realtime-
// bridge" by default) — completely separate from worker.js's
// "varshanet-scoring-workers" group on the *different* `raw-reports`
// topic. Different topic, different group: no interaction between the
// two, and running several web-process instances behind a load balancer
// is fine too, since each just gets its own copy of every ACTIVITY message
// (this is a broadcast/fan-out use of Kafka, not a work-queue one — we
// WANT every instance, and therefore every connected browser, to see
// every message, not have Kafka split them up).
//
// Unauthenticated on purpose: the only thing broadcast here is report
// data already visible to anyone via GET /api/reports — this isn't a new
// data exposure, just a faster way to deliver the same data.

const { WebSocketServer } = require("ws");
const { createConsumer } = require("./kafka");
const { ACTIVITY } = require("./topics");

const REALTIME_GROUP_ID = process.env.KAFKA_REALTIME_GROUP || "varshanet-realtime-bridge";

function attachRealtime(httpServer) {
  const wss = new WebSocketServer({ server: httpServer, path: "/ws" });
  const clients = new Set();

  wss.on("connection", (ws) => {
    clients.add(ws);
    ws.on("close", () => clients.delete(ws));
    ws.on("error", () => clients.delete(ws));
  });

  function broadcast(raw) {
    for (const ws of clients) {
      if (ws.readyState === ws.OPEN) {
        ws.send(raw);
      }
    }
  }

  (async () => {
    const consumer = createConsumer(REALTIME_GROUP_ID);
    try {
      await consumer.connect();
      // fromBeginning: false — a newly-opened browser tab already gets the
      // full current state via GET /api/reports on load; it only needs
      // this stream for what happens FROM NOW ON, not a replay of every
      // report ever created.
      await consumer.subscribe({ topic: ACTIVITY, fromBeginning: false });
      await consumer.run({
        eachMessage: async ({ message }) => {
          if (!message.value) return;
          broadcast(message.value.toString());
        },
      });
      console.log(`Realtime WebSocket bridge live at /ws (Kafka group "${REALTIME_GROUP_ID}")`);
    } catch (e) {
      console.error(
        "Realtime WebSocket bridge failed to start (dashboard will still work via polling fallback):",
        e.message
      );
    }
  })();

  return wss;
}

module.exports = { attachRealtime };

require("dotenv").config();

const express = require("express");
const cors = require("cors");
const path = require("path");
const http = require("http");

const db = require("./db");
const { generateSeedReports } = require("./seedData");
const { runIngestion } = require("./ingest");
const { runSachetIngestion } = require("./sachetIngest");
const { runImdCapIngestion } = require("./imdCapIngest");
const { attachRealtime } = require("./realtime");

const reportsRouter = require("./routes/reports");
const adminRouter = require("./routes/admin");
const weatherRouter = require("./routes/weather");
const attachReportSearch = require("./searchrouter");

const app = express();

app.use(cors());
app.use(express.json());

app.use("/api/reports", reportsRouter);
app.use("/api/admin", adminRouter);
app.use("/api/weather", weatherRouter);
attachReportSearch(app);

// Serve the frontend
const PUBLIC_DIR = path.join(__dirname, "..", "public");
app.use(express.static(PUBLIC_DIR));
app.get("*", (req, res) => {
  res.sendFile(path.join(PUBLIC_DIR, "index.html"));
});

async function main() {
  // Auto-seed demo history on first boot so the dashboard isn't empty.
  // No-ops once the reports collection already has rows in it.
  const existing = await db.getReportsCount();
  if (existing === 0) {
    const seeded = generateSeedReports(220);
    await db.bulkSeed(seeded);
    console.log(`Seeded ${seeded.length} demo reports into MongoDB`);
  }

  // Bootstraps the first admin account from env vars so existing setups
  // keep working — no-ops once any admin already exists in the database.
  if (
    process.env.ADMIN_USERNAME &&
    process.env.ADMIN_PASSWORD_HASH &&
    !process.env.ADMIN_PASSWORD_HASH.startsWith("paste_")
  ) {
    const seededAdmin = await db.seedDefaultAdminIfEmpty(
      process.env.ADMIN_USERNAME,
      process.env.ADMIN_PASSWORD_HASH
    );
    if (seededAdmin) {
      console.log(`Seeded default admin account "${process.env.ADMIN_USERNAME}" into MongoDB`);
    }
  }

  const PORT = process.env.PORT || 3000;
  // A plain http.Server (not app.listen()'s implicit one) so the WebSocket
  // upgrade handshake in attachRealtime() can share the same port as the
  // Express app — the dashboard connects to ws://<same host>/ws, no
  // second port or CORS setup needed.
  const server = http.createServer(app);
  attachRealtime(server);
  server.listen(PORT, () => {
    console.log(`VarshaNet server running on http://localhost:${PORT}`);
  });

  // Live public-API ingestion: pull real Open-Meteo readings for major
  // Indian cities once at boot, then every 20 minutes.
  runIngestion();
  const INGEST_INTERVAL_MS = 20 * 60 * 1000;
  setInterval(runIngestion, INGEST_INTERVAL_MS);

  // Live public-dataset ingestion: pull real NDMA SACHET disaster/weather
  // alerts once at boot, then every 30 minutes.
  runSachetIngestion();
  const SACHET_INTERVAL_MS = 30 * 60 * 1000;
  setInterval(runSachetIngestion, SACHET_INTERVAL_MS);

  // Live ingestion straight from IMD's own official alert feed, once at
  // boot then every 30 minutes.
  runImdCapIngestion();
  const IMD_CAP_INTERVAL_MS = 30 * 60 * 1000;
  setInterval(runImdCapIngestion, IMD_CAP_INTERVAL_MS);

  // Note: this project previously also ran two live social-media sources
  // (Bluesky, then Mastodon — see git history / mastodonIngest.js if still
  // present for reference). Both have been removed; IMD, SACHET, and the
  // Open-Meteo weather feed above are the live sources now.

  // Optional local-dev convenience: set RUN_WORKER_INPROCESS=true to also
  // start the Kafka consumer (server/worker.js) inside this same process,
  // so `npm start` alone is enough to see queued reports actually get
  // scored and appear in the dashboard without a second terminal running
  // `npm run worker`. Leave this unset in any real deployment — the whole
  // point of the worker being a separate process is that it scales
  // independently of the web service (see server/worker.js's header
  // comment and README.md's Architecture section).
  if ((process.env.RUN_WORKER_INPROCESS || "").toLowerCase() === "true") {
    console.log("RUN_WORKER_INPROCESS=true — starting the Kafka consumer inside the web process too.");
    require("./worker");
  }
}

main().catch((err) => {
  console.error("Fatal startup error:", err);
  process.exit(1);
});
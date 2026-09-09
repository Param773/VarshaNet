require("dotenv").config();

const express = require("express");
const cors = require("cors");
const path = require("path");

const db = require("./db");
const { generateSeedReports } = require("./seedData");
const { runIngestion } = require("./ingest");
const { runSachetIngestion } = require("./sachetIngest");
const { runImdCapIngestion } = require("./imdCapIngest");
const { runSocialIngestion } = require("./socialIngest");
const { runMastodonIngestion } = require("./mastodonIngest");

const reportsRouter = require("./routes/reports");
const adminRouter = require("./routes/admin");
const weatherRouter = require("./routes/weather");

const app = express();

app.use(cors());
app.use(express.json());

app.use("/api/reports", reportsRouter);
app.use("/api/admin", adminRouter);
app.use("/api/weather", weatherRouter);

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
  app.listen(PORT, () => {
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

  // Live social-media ingestion (Reddit's free public search — see
  // socialIngest.js for why Reddit instead of Twitter/X). Runs more often
  // than the other feeds since social posts refresh faster than
  // government alerts, once at boot then every 15 minutes.
  runSocialIngestion();
  const SOCIAL_INTERVAL_MS = 15 * 60 * 1000;
  setInterval(runSocialIngestion, SOCIAL_INTERVAL_MS);

  // Second live social-media source: Mastodon's free, keyless public
  // hashtag-timeline API (see mastodonIngest.js for why it, and not
  // Bluesky, was picked as the Reddit adapter's sibling). Same 15-minute
  // cadence as Reddit since it's the same kind of fast-refreshing source.
  runMastodonIngestion();
  const MASTODON_INTERVAL_MS = 15 * 60 * 1000;
  setInterval(runMastodonIngestion, MASTODON_INTERVAL_MS);

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
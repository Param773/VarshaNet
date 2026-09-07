require("dotenv").config();

const express = require("express");
const cors = require("cors");
const path = require("path");

const db = require("./db");
const { generateSeedReports } = require("./seedData");
const { runIngestion } = require("./ingest");
const { runSachetIngestion } = require("./sachetIngest");
const { runImdCapIngestion } = require("./imdCapIngest");

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
}

main().catch((err) => {
  console.error("Fatal startup error:", err);
  process.exit(1);
});
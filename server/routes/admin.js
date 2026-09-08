const express = require("express");
const bcrypt = require("bcryptjs");
const jwt = require("jsonwebtoken");

const db = require("../db");
const { requireAdmin } = require("../middleware/auth");
const { runIngestion } = require("../ingest");
const { runSachetIngestion } = require("../sachetIngest");
const { runImdCapIngestion } = require("../imdCapIngest");
const { runSocialIngestion } = require("../socialIngest");

const router = express.Router();

router.post("/login", async (req, res) => {
  const { username, password } = req.body || {};
  if (!username || !password) {
    return res.status(400).json({ error: "Username and password are required" });
  }

  const admin = await db.getAdminByUsername(username);
  if (!admin) {
    return res.status(401).json({ error: "Invalid credentials" });
  }

  const ok = await bcrypt.compare(password, admin.passwordHash);
  if (!ok) return res.status(401).json({ error: "Invalid credentials" });

  const token = jwt.sign({ username }, process.env.JWT_SECRET, { expiresIn: "12h" });
  res.json({ token });
});
router.post("/ingest", requireAdmin, async (req, res) => {
  try {
    const [weatherReports, sachetReports, imdReports, socialReports] = await Promise.all([
      runIngestion(),
      runSachetIngestion(),
      runImdCapIngestion(),
      runSocialIngestion(),
    ]);
    const reports = [...weatherReports, ...sachetReports, ...imdReports, ...socialReports];
    res.json({
      created: reports.length,
      reports,
      breakdown: {
        weatherApi: weatherReports.length,
        publicDataset: sachetReports.length,
        imdApi: imdReports.length,
        socialMedia: socialReports.length,
      },
    });
  } catch (e) {
    console.error("Manual ingestion failed:", e);
    res.status(500).json({ error: "Ingestion failed. Please try again." });
  }
});

router.get("/admins", requireAdmin, async (req, res) => {
  try {
    const admins = await db.listAdminUsernames();
    res.json({ admins });
  } catch (e) {
    console.error("Failed to list admins:", e);
    res.status(500).json({ error: "Failed to load admin list." });
  }
});

router.post("/admins", requireAdmin, async (req, res) => {
  const { username, password } = req.body || {};
  if (!username || !password) {
    return res.status(400).json({ error: "Username and password are required" });
  }
  if (password.length < 6) {
    return res.status(400).json({ error: "Password must be at least 6 characters." });
  }
  try {
    const passwordHash = await bcrypt.hash(password, 10);
    const admin = await db.createAdmin(username.trim(), passwordHash);
    res.json({ username: admin.username });
  } catch (e) {
    res.status(400).json({ error: e.message || "Failed to create admin." });
  }
});

module.exports = router;
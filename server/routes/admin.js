const express = require("express");
const bcrypt = require("bcryptjs");
const jwt = require("jsonwebtoken");

const db = require("../db");
const { requireAdmin, requireRole } = require("../middleware/auth");
const { runIngestion } = require("../ingest");
const { runSachetIngestion } = require("../sachetIngest");
const { runImdCapIngestion } = require("../imdCapIngest");

const router = express.Router();

const VALID_ROLES = ["admin", "moderator", "analyst"];

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

  const token = jwt.sign({ username, role: admin.role }, process.env.JWT_SECRET, {
    expiresIn: "12h",
  });
  res.json({ token, role: admin.role });
});

// Pulling live data changes the database, so "analyst" (read-only) can't
// trigger it — only "admin" and "moderator" can.
router.post("/ingest", requireAdmin, requireRole("admin", "moderator"), async (req, res) => {
  try {
    const [weatherReports, sachetReports, imdReports] =
      await Promise.all([
        runIngestion(),
        runSachetIngestion(),
        runImdCapIngestion(),
      ]);
    const reports = [
      ...weatherReports,
      ...sachetReports,
      ...imdReports,
    ];
    await db.addAuditLog({
      actor: req.admin.username,
      action: "manual_ingest",
      targetType: "system",
      targetId: null,
      detail: `Pulled live data: ${reports.length} report(s) queued for processing`,
    });
    res.json({
      // These counts mean "published onto the Kafka stream", not
      // "already scored and saved" — server/worker.js's consumer group
      // does that asynchronously, usually within a second or two. See
      // README.md's Architecture section.
      created: reports.length,
      queued: true,
      reports,
      breakdown: {
        weatherApi: weatherReports.length,
        publicDataset: sachetReports.length,
        imdApi: imdReports.length,
      },
    });
  } catch (e) {
    console.error("Manual ingestion failed:", e);
    res.status(500).json({ error: "Ingestion failed. Please try again." });
  }
});

// GET /api/admin/stats — dashboard totals/insights computed across the
// WHOLE reports collection via MongoDB aggregation, not just the capped
// recent-500 window that GET /api/reports returns. See db.js for why.
router.get("/stats", requireAdmin, async (req, res) => {
  try {
    const stats = await db.getReportStats();
    res.json(stats);
  } catch (e) {
    console.error("Failed to load admin stats:", e);
    res.status(500).json({ error: "Failed to load stats." });
  }
});

// Any signed-in admin (any role) can see who's on the team — transparency
// about who holds which role isn't itself a sensitive action.
router.get("/admins", requireAdmin, async (req, res) => {
  try {
    const admins = await db.listAdminUsernames();
    res.json({ admins });
  } catch (e) {
    console.error("Failed to list admins:", e);
    res.status(500).json({ error: "Failed to load admin list." });
  }
});

// Only a full "admin" can create new admin accounts — a moderator or
// analyst granting themselves (or anyone else) more access would defeat
// the point of having tiers at all.
router.post("/admins", requireAdmin, requireRole("admin"), async (req, res) => {
  const { username, password, role } = req.body || {};
  if (!username || !password) {
    return res.status(400).json({ error: "Username and password are required" });
  }
  if (password.length < 6) {
    return res.status(400).json({ error: "Password must be at least 6 characters." });
  }
  const finalRole = role || "moderator";
  if (!VALID_ROLES.includes(finalRole)) {
    return res.status(400).json({ error: `Role must be one of: ${VALID_ROLES.join(", ")}` });
  }
  try {
    const passwordHash = await bcrypt.hash(password, 10);
    const admin = await db.createAdmin(username.trim(), passwordHash, finalRole);
    await db.addAuditLog({
      actor: req.admin.username,
      action: "admin_created",
      targetType: "admin",
      targetId: admin.username,
      detail: `Created ${finalRole} account "${admin.username}"`,
    });
    res.json({ username: admin.username, role: admin.role });
  } catch (e) {
    res.status(400).json({ error: e.message || "Failed to create admin." });
  }
});

// Only a full "admin" can remove admin accounts — same reasoning as
// creating them. Two extra guardrails on top of the role check: you can't
// delete your own account (stops an accidental mid-session lockout), and
// the last remaining "admin"-role account can't be deleted either, so
// there's always at least one person left who can manage the team.
router.delete("/admins/:username", requireAdmin, requireRole("admin"), async (req, res) => {
  const { username } = req.params;
  if (username === req.admin.username) {
    return res.status(400).json({ error: "You can't delete your own account." });
  }
  try {
    const target = await db.getAdminByUsername(username);
    if (!target) {
      return res.status(404).json({ error: "Admin not found." });
    }
    if (target.role === "admin") {
      const admins = await db.listAdminUsernames();
      const adminCount = admins.filter((a) => a.role === "admin").length;
      if (adminCount <= 1) {
        return res.status(400).json({ error: "Can't delete the last remaining Admin account." });
      }
    }
    await db.deleteAdmin(username);
    await db.addAuditLog({
      actor: req.admin.username,
      action: "admin_deleted",
      targetType: "admin",
      targetId: username,
      detail: `Removed ${target.role || "admin"} account "${username}"`,
    });
    res.json({ username });
  } catch (e) {
    console.error("Failed to delete admin:", e);
    res.status(500).json({ error: "Failed to delete admin." });
  }
});

// Read-only audit trail — every role can view it (seeing the log isn't a
// privileged action, only *acting* is), so the console can show everyone
// the same accountability trail.
router.get("/audit-logs", requireAdmin, async (req, res) => {
  try {
    const logs = await db.getAuditLogs();
    res.json({ logs });
  } catch (e) {
    console.error("Failed to load audit logs:", e);
    res.status(500).json({ error: "Failed to load audit logs." });
  }
});

module.exports = router;

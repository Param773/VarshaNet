const express = require("express");
const bcrypt = require("bcryptjs");
const jwt = require("jsonwebtoken");

const db = require("../db");
const { requireAdmin, requireRole } = require("../middleware/auth");
const { runIngestion } = require("../ingest");
const { runSachetIngestion } = require("../sachetIngest");
const { runImdCapIngestion } = require("../imdCapIngest");
const { runMastodonIngestion } = require("../mastodonIngest");
const { scoreReport, statusFromTrust } = require("../scoring");

const router = express.Router();

const VALID_ROLES = ["admin", "moderator"];

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

// Pulling live data changes the database — only "admin" and "moderator"
// can trigger it.
router.post("/ingest", requireAdmin, requireRole("admin", "moderator"), async (req, res) => {
  try {
    const [weatherReports, sachetReports, imdReports, mastodonReports] =
      await Promise.all([
        runIngestion(),
        runSachetIngestion(),
        runImdCapIngestion(),
        runMastodonIngestion(),
      ]);
    const reports = [
      ...weatherReports,
      ...sachetReports,
      ...imdReports,
      ...mastodonReports,
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
        socialMedia: mastodonReports.length,
      },
    });
  } catch (e) {
    console.error("Manual ingestion failed:", e);
    res.status(500).json({ error: "Ingestion failed. Please try again." });
  }
});

// One-time migration endpoint — re-scores every existing report from an
// automated feed source (IMD API, Public Dataset / NDMA SACHET, Weather
// API) using the fixed, source-aware scoreReport() logic (see
// server/scoring.js for why those sources were being under-scored).
// Exists as an admin route rather than requiring shell access, since
// Render's free tier doesn't offer a Shell. Doesn't delete or fabricate
// anything — only recomputes trust/status from each report's own existing
// text/event/media, exactly as a fresh ingest would today. Safe to call
// more than once (idempotent) if you ever need to re-run it.
router.post("/rescore-official", requireAdmin, requireRole("admin", "moderator"), async (req, res) => {
  try {
    const SOURCES_TO_RESCORE = ["IMD API", "Public Dataset", "Weather API"];
    const reports = await db.getReportsBySource(SOURCES_TO_RESCORE);

    const toUpdate = [];
    let unchanged = 0;

    for (const r of reports) {
      const { trustScore } = scoreReport({
        description: r.text,
        event: r.event,
        source: r.source,
        hasMedia: !!(r.hasPhoto || r.hasVideo),
        mediaReused: false,
        officialMain: null,
        city: r.city,
      });

      // A confirmed duplicate must stay "flagged" regardless of score,
      // same rule as at ingest time (see worker.js) — never let a
      // re-score silently un-flag a known duplicate.
      const isConfirmedDuplicate = r.duplicateOf !== null && r.duplicateOf !== undefined;
      const newStatus = isConfirmedDuplicate ? "flagged" : statusFromTrust(trustScore);

      if (trustScore !== r.trust || newStatus !== r.status) {
        toUpdate.push({ id: r.id, trust: trustScore, status: newStatus });
      } else {
        unchanged += 1;
      }
    }

    // One bulk request instead of one round-trip per report — with
    // thousands of existing reports, doing them one at a time risked
    // Render's free-tier proxy timing the request out before it finished.
    const { modifiedCount } = await db.bulkUpdateReportTrust(toUpdate);

    await db.addAuditLog({
      actor: req.admin.username,
      action: "rescore_official_reports",
      targetType: "system",
      targetId: null,
      detail: `Rescored official-feed reports: ${modifiedCount} updated, ${unchanged} already correct (of ${reports.length} checked)`,
    });

    res.json({ checked: reports.length, updated: modifiedCount, unchanged });
  } catch (e) {
    console.error("Rescore-official failed:", e);
    res.status(500).json({ error: "Rescore failed. Please try again." });
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

// GET /api/admin/queue — every report the Review Queue tabs need to act
// on, across the WHOLE collection (not the capped recent-500 window
// GET /api/reports returns) — see db.getQueueReports for why this needs
// to be unbounded.
router.get("/queue", requireAdmin, async (req, res) => {
  try {
    const reports = await db.getQueueReports();
    res.json(reports);
  } catch (e) {
    console.error("Failed to load review queue:", e);
    res.status(500).json({ error: "Failed to load review queue." });
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

// Only a full "admin" can create new admin accounts — a moderator
// granting themselves (or anyone else) more access would defeat
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

const express = require("express");
const multer = require("multer");
const crypto = require("crypto");
const path = require("path");

const db = require("../db");
const { scoreReport, statusFromTrust, detectCategory } = require("../scoring");
const { fetchCityWeather } = require("../weather");
const { requireAdmin, requireRole } = require("../middleware/auth");
const { rateLimit } = require("../middleware/rateLimit");
const { botTrap } = require("../middleware/botTrap");
const { generateCaptcha, verifyCaptcha } = require("../middleware/captcha");
const { computePerceptualHash } = require("../perceptualHash");
const { assessImagePlausibility } = require("../imageAuthenticity");
const { verifyPhotoLocation } = require("../exifGeoCheck");
const { uploadBuffer } = require("../cloudinaryUpload");
const { getProducer } = require("../kafka");
const { ACTIVITY } = require("../topics");

const router = express.Router();

// Kept at 5MB (down from 15MB): every upload is fully buffered into RAM by
// multer, and photos are then fully decoded to raw pixel data for the
// perceptual hash — a high-resolution photo can decode to several times its
// compressed file size in memory. This bounds the worst case; it doesn't
// eliminate it (a well-compressed 5MB photo can still be high-resolution),
// but it materially shrinks it without needing a different image library.
const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 5 * 1024 * 1024 }, // 5MB
});

// GET /api/reports — most recent N reports (client filters/sorts them),
// same array shape the original prototype held in memory. Bounded by
// default so this can't keep growing forever as the collection grows —
// pass ?limit= for more (capped server-side in db.js).
router.get("/", async (req, res) => {
  const limit = parseInt(req.query.limit, 10);
  res.json(await db.getAllReports(Number.isNaN(limit) ? {} : { limit }));
});

// GET /api/reports/public-stats — the 4 real numbers behind the landing
// page's hero counters (reports ingested, cities covered, verification
// rate, suspicious/fake reports filtered). Unauthenticated on purpose —
// same audience as the landing page itself — and returns only these 4
// aggregate numbers, nothing per-report, unlike GET / above.
router.get("/public-stats", async (req, res) => {
  try {
    res.json(await db.getPublicStats());
  } catch (e) {
    console.error("Failed to load public stats:", e);
    res.status(500).json({ error: "Failed to load stats." });
  }
});

// GET /api/reports/captcha — issues a fresh math-challenge question +
// encrypted token for the report form (see server/middleware/captcha.js).
// Lighter/separate limiter from the submit one below: a user re-fetching
// this a few times (typo, expired token, form left open) shouldn't burn
// into their 5-submissions/10min budget.
const captchaIssueLimiter = rateLimit({
  windowMs: 60 * 1000,
  max: 20,
  message: "Too many captcha requests — please slow down.",
});

router.get("/captcha", captchaIssueLimiter, (req, res) => {
  res.json(generateCaptcha());
});

// POST /api/reports — citizen submits a report. multipart/form-data:
//   category, description, city, state, lat?, lng?, media? (file),
//   website (honeypot), formLoadedAt (bot-trap timing), captchaToken/captchaAnswer
const reportSubmitLimiter = rateLimit({
  windowMs: 10 * 60 * 1000,
  max: 5,
  message: "Too many reports submitted from this connection. Please try again in a few minutes.",
});

router.post("/", reportSubmitLimiter, (req, res, next) => {
  upload.single("media")(req, res, (err) => {
    if (err instanceof multer.MulterError && err.code === "LIMIT_FILE_SIZE") {
      return res.status(400).json({ error: "That file is too large — please upload media under 5MB." });
    }
    if (err) return next(err);
    next();
  });
}, botTrap, verifyCaptcha, async (req, res) => {
  try {
    const { category, description, city, state: stateName, lat, lng } = req.body || {};
    if (!category || !city) {
      return res.status(400).json({ error: "category and city are required" });
    }

    const file = req.file;
    let mediaHash = null;
    let mediaPath = null;
    let mediaUrl = null;
    let hasPhoto = false;
    let hasVideo = false;
    const hasMedia = !!file;

    if (file) {
      hasPhoto = file.mimetype.indexOf("image") === 0;
      hasVideo = file.mimetype.indexOf("video") === 0;
      mediaHash = crypto.createHash("sha256").update(file.buffer).digest("hex");
      const ext = path.extname(file.originalname || "") || "";
      mediaPath = `${mediaHash}${ext}`;
      mediaUrl = await uploadBuffer(file.buffer, hasVideo ? "video" : "image");
    }

    // Duplicate detection by actual file content, not just name/size.
    const existingWithHash = mediaHash ? await db.findByMediaHash(mediaHash) : null;

    const NEAR_DUPLICATE_MAX_DISTANCE = 10;
    let perceptualHash = null;
    let nearDuplicateMatch = null;
    let imageAssessment = null;
    if (file && hasPhoto) {
      try {
        perceptualHash = await computePerceptualHash(file.buffer);
        if (!existingWithHash) {
          nearDuplicateMatch = await db.findNearDuplicateByPerceptualHash(
            perceptualHash,
            NEAR_DUPLICATE_MAX_DISTANCE
          );
        }
      } catch (e) {
        console.error("Perceptual hash failed:", e.message);
      }
      // Best-effort — resolves to null (no opinion) if ANTHROPIC_API_KEY
      // isn't set or the call fails/times out. Only run for images, not
      // video (see imageAuthenticity.js), and only when a photo was
      // actually attached — no point spending the call otherwise.
      imageAssessment = await assessImagePlausibility({
        imageBuffer: file.buffer,
        mimeType: file.mimetype,
        event: category,
        city,
      });
    }
    // Cross-check against live weather for the named city. If the lookup
    // fails (bad spelling, network hiccup) scoring just proceeds without it,
    // same fallback behaviour as the original prototype.
    let officialMain = null;
    let geoLat = lat !== undefined && lat !== "" ? parseFloat(lat) : null;
    let geoLng = lng !== undefined && lng !== "" ? parseFloat(lng) : null;
    try {
      const weather = await fetchCityWeather(city);
      officialMain = weather.main;
      if (geoLat === null) geoLat = weather.lat;
      if (geoLng === null) geoLng = weather.lng;
    } catch (e) {
      // no live weather available — that's fine, scoreReport tolerates null
    }
    // Cross-verify the photo's embedded EXIF GPS (if any) against the
    // location reported for this submission — deliberately done BEFORE
    // the random-jitter fallback below runs, so this only ever compares
    // against a real location (browser GPS or the weather API's city
    // center), never against a made-up point on the map.
    let photoLocationCheck = null;
    if (file && hasPhoto) {
      try {
        photoLocationCheck = verifyPhotoLocation({
          imageBuffer: file.buffer,
          mimeType: file.mimetype,
          reportedLat: geoLat,
          reportedLng: geoLng,
        });
      } catch (e) {
        console.error("EXIF GPS check failed:", e.message);
      }
    }

    if (geoLat === null || Number.isNaN(geoLat)) geoLat = 22.5 + (Math.random() - 0.5) * 16;
    if (geoLng === null || Number.isNaN(geoLng)) geoLng = 80 + (Math.random() - 0.5) * 16;

    const { trustScore, reasons } = scoreReport({
      description,
      event: category,
      hasMedia,
      mediaReused: !!existingWithHash,
      mediaNearDuplicate: !!nearDuplicateMatch,
      imageAssessment,
      photoLocationCheck,
      officialMain,
      city,
    });
    const status = statusFromTrust(trustScore);
    // Same rule as worker.js's Kafka consumer path: "pending" is the only
    // undecided state — the auto-resolve sweep (server/autoResolve.js) is
    // what eventually moves it via corroboration or the 6h timeout.
    const decidedBy = status === "pending" ? null : "AI (initial score)";

    // Rule-based auto-categorization from free text, independent of what the
    // citizen picked in the dropdown. Stored alongside the report so the
    // admin console can flag a mismatch between reported vs detected event.
    const autoCategory = detectCategory(description);

    const report = await db.addReport({
      city,
      state: stateName || "Unknown",
      lat: geoLat,
      lng: geoLng,
      event: category,
      autoCategory,
      source: "Citizen Report App",
      ts: Date.now(),
      trust: trustScore,
      status,
      decidedBy,
      hasPhoto,
      hasVideo,
      text: description || "(no description provided)",
      duplicateOf: existingWithHash
        ? existingWithHash.id
        : nearDuplicateMatch
        ? nearDuplicateMatch.id
        : null,
      mediaHash,
      mediaPath,
      mediaUrl,
      perceptualHash,
    });

    // Every report — auto-ingested or citizen-submitted — now touches the
    // same Kafka stream (see server/kafka.js, server/topics.js). This one's
    // already scored and saved synchronously above for the immediate API
    // response the frontend expects; publishing here is purely for the
    // dashboard's real-time WebSocket bridge (server/index.js), so it's
    // deliberately fire-and-forget — a slow or unreachable broker must
    // never delay or break a citizen's submission.
    getProducer()
      .then((producer) =>
        producer.send({
          topic: ACTIVITY,
          messages: [{ key: report.city || "unknown", value: JSON.stringify({ type: "report_created", report }) }],
        })
      )
      .catch((e) => console.error("Failed to publish activity event:", e.message));

    res.json({ report, reasons, autoCategory, nearDuplicate: !!nearDuplicateMatch });

  } catch (err) {
    console.error("Failed to submit report:", err);
    res.status(500).json({ error: "Failed to submit report. Please try again." });
  }
});

// PATCH /api/reports/:id/status — admin approves or rejects a queued report.
// "analyst" is read-only by design, so it's excluded here — only "admin"
// and "moderator" can actually change a report's status.
router.patch("/:id/status", requireAdmin, requireRole("admin", "moderator"), async (req, res) => {
  const id = parseInt(req.params.id, 10);
  const { status } = req.body || {};
  if (!["verified", "rejected"].includes(status)) {
    return res.status(400).json({ error: 'status must be "verified" or "rejected"' });
  }
  const updated = await db.updateReportStatus(id, status, "admin");
  if (!updated) return res.status(404).json({ error: "Report not found" });
  await db.addAuditLog({
    actor: req.admin.username,
    action: status === "verified" ? "approved" : "rejected",
    targetType: "report",
    targetId: id,
    detail: `${updated.city}, ${updated.state} — ${updated.event}`,
  });
  // Same real-time bridge as report creation — a manual admin/moderator
  // decision should reach every open dashboard tab immediately, not just
  // the browser that clicked the button.
  getProducer()
    .then((producer) =>
      producer.send({
        topic: ACTIVITY,
        messages: [{ key: updated.city || "unknown", value: JSON.stringify({ type: "report_updated", report: updated }) }],
      })
    )
    .catch((e) => console.error("Failed to publish activity event:", e.message));
  res.json(updated);
});

module.exports = router;

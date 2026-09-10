const crypto = require("crypto");

// No external CAPTCHA provider (reCAPTCHA/hCaptcha) here on purpose — those
// need a signup + site/secret key pair the team would have to go register
// for. This is a small in-house arithmetic challenge instead: the question
// is plain text, but the *answer* + expiry travel to the client sealed
// inside an AES-256-GCM encrypted token, so a bot can't just read the
// answer back out of the token the way it could from an unsigned/unencrypted
// one. Verification is stateless (no server-side challenge store), which
// matters here specifically because the API runs as multiple worker
// instances behind a load balancer (see server/worker.js, server/kafka.js) —
// same reasoning as the JWT admin sessions in auth.js.
const SECRET = process.env.CAPTCHA_SECRET || process.env.JWT_SECRET || "dev-only-captcha-secret";
const KEY = crypto.createHash("sha256").update(SECRET).digest(); // 32 bytes for AES-256
const CAPTCHA_TTL_MS = 5 * 60 * 1000; // 5 minutes to solve before it expires

function randInt(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

function encryptPayload(payload) {
  const iv = crypto.randomBytes(12);
  const cipher = crypto.createCipheriv("aes-256-gcm", KEY, iv);
  const encrypted = Buffer.concat([cipher.update(JSON.stringify(payload), "utf8"), cipher.final()]);
  const authTag = cipher.getAuthTag();
  return Buffer.concat([iv, authTag, encrypted]).toString("base64url");
}

function decryptToken(token) {
  try {
    const buf = Buffer.from(token, "base64url");
    const iv = buf.subarray(0, 12);
    const authTag = buf.subarray(12, 28);
    const encrypted = buf.subarray(28);
    const decipher = crypto.createDecipheriv("aes-256-gcm", KEY, iv);
    decipher.setAuthTag(authTag);
    const decrypted = Buffer.concat([decipher.update(encrypted), decipher.final()]).toString("utf8");
    return JSON.parse(decrypted);
  } catch (e) {
    return null; // tampered, malformed, or wrong-key token
  }
}

// GET /api/reports/captcha handler body — returns a fresh question + token.
function generateCaptcha() {
  const a = randInt(1, 20);
  const b = randInt(1, 20);
  const subtract = Math.random() < 0.5 && a >= b; // avoid negative answers
  const answer = subtract ? a - b : a + b;
  const question = `${a} ${subtract ? "−" : "+"} ${b} = ?`;
  const token = encryptPayload({ a: answer, exp: Date.now() + CAPTCHA_TTL_MS });
  return { question, token };
}

// Express middleware — pairs with botTrap as a second, orthogonal layer:
// botTrap catches naive bots that never render/wait for the form; this
// catches scripted submitters that skip actually solving the challenge.
// Note: a captured valid (token, answer) pair could in principle be
// replayed until the 5-minute expiry — there's no single-use tracking,
// since that would need a shared store across worker instances. The
// per-IP rate limiter (rateLimit.js) already bounds how much that's worth
// to an attacker, so this is an acceptable tradeoff for this scale.
function verifyCaptcha(req, res, next) {
  const { captchaToken, captchaAnswer } = req.body || {};
  if (!captchaToken || captchaAnswer === undefined || captchaAnswer === "") {
    return res.status(400).json({ error: "Please answer the captcha." });
  }
  const payload = decryptToken(captchaToken);
  if (!payload || Date.now() > payload.exp) {
    return res.status(400).json({ error: "Captcha expired — please try again." });
  }
  if (parseInt(captchaAnswer, 10) !== payload.a) {
    return res.status(400).json({ error: "Incorrect captcha answer — please try again." });
  }
  next();
}

module.exports = { generateCaptcha, verifyCaptcha };

// AI-based photo plausibility check for citizen-submitted media.
//
// Every other trust-score factor in server/scoring.js is trivially easy
// to satisfy (get the category keyword right in the text, "GPS captured"
// happens automatically, attach ANY photo at all) — none of them actually
// look at what was uploaded. This is the one check that does: it asks
// Claude's vision to judge whether the image plausibly shows real-world
// outdoor evidence of the claimed weather event, and to flag the obvious
// tells when it doesn't (a screenshot, a watermarked/stock photo, an
// indoor or unrelated scene). It's a heuristic, not a forensic tool — a
// careful bad actor can still beat it — but it closes the gap where any
// random photo (e.g. downloaded from a search engine) scored the same
// "+10, report includes photo evidence" as a genuine one.
//
// If ANTHROPIC_API_KEY isn't set, or the call fails, times out, or
// returns something we can't parse, assessImagePlausibility resolves to
// null — server/scoring.js treats that as "no opinion" (neutral), the
// same graceful-degradation pattern as server/weather.js and
// server/cloudinaryUpload.js already use for their own external calls. A
// citizen's submission must never be blocked, rejected, or meaningfully
// delayed just because this optional check is unavailable.

const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY;
const MODEL = "claude-sonnet-4-6";
const TIMEOUT_MS = 15000;

const EVENT_LABELS = {
  flooding: "flooding / waterlogging",
  thunderstorm: "a thunderstorm (lightning, storm clouds)",
  rainfall: "rainfall",
  heatwave: "a heatwave (e.g. sun-scorched, heat-haze conditions)",
  fog: "fog / low visibility",
  dust_storm: "a dust storm",
  strong_winds: "strong winds (e.g. debris, bent trees, wind damage)",
};

function describeEvent(event) {
  return EVENT_LABELS[event] || event || "a weather event";
}

// Strips a ```json fence if the model added one despite being asked not
// to — matches the defensive parsing pattern used elsewhere in this repo
// (see the Claude-in-Claude guidance this project's README references).
function parseAssessment(rawText) {
  const cleaned = rawText.replace(/```json|```/g, "").trim();
  const parsed = JSON.parse(cleaned);
  if (typeof parsed.plausible !== "boolean") return null;
  const confidence = Number.isFinite(parsed.confidence)
    ? Math.max(0, Math.min(100, Math.round(parsed.confidence)))
    : 50;
  return {
    plausible: parsed.plausible,
    confidence,
    reason: String(parsed.reason || "").slice(0, 200),
  };
}

/**
 * @param {Object} o
 * @param {Buffer} o.imageBuffer
 * @param {string} o.mimeType   - must be an image/* mime type
 * @param {string} o.event      - category key, e.g. "flooding"
 * @param {string} o.city
 * @returns {Promise<{plausible: boolean, confidence: number, reason: string} | null>}
 */
async function assessImagePlausibility({ imageBuffer, mimeType, event, city }) {
  if (!ANTHROPIC_API_KEY || !imageBuffer || !mimeType) return null;

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), TIMEOUT_MS);

  try {
    const base64 = imageBuffer.toString("base64");
    const prompt =
      `A citizen submitted this photo as evidence of ${describeEvent(event)} in ${city || "an Indian city"}. ` +
      `Judge only what the image itself shows — you have no other context and can't know if it's recent. ` +
      `Reply with ONLY this JSON, no other text: ` +
      `{"plausible": true or false, "confidence": 0-100, "reason": "one short sentence"}. ` +
      `Set "plausible" to false if the image is clearly a screenshot, a stock or watermarked photo, ` +
      `an indoor scene, or otherwise doesn't look like real outdoor evidence of this kind of weather event. ` +
      `Otherwise true. Be concise and decisive.`;

    const res = await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      signal: controller.signal,
      headers: {
        "Content-Type": "application/json",
        "x-api-key": ANTHROPIC_API_KEY,
        "anthropic-version": "2023-06-01",
      },
      body: JSON.stringify({
        model: MODEL,
        max_tokens: 200,
        messages: [
          {
            role: "user",
            content: [
              { type: "image", source: { type: "base64", media_type: mimeType, data: base64 } },
              { type: "text", text: prompt },
            ],
          },
        ],
      }),
    });

    if (!res.ok) {
      console.error(`Image plausibility check failed: HTTP ${res.status}`);
      return null;
    }

    const data = await res.json();
    const textBlock = (data.content || []).find((c) => c.type === "text");
    if (!textBlock) return null;

    return parseAssessment(textBlock.text);
  } catch (e) {
    console.error("Image plausibility check failed:", e.message);
    return null;
  } finally {
    clearTimeout(timeout);
  }
}

module.exports = { assessImagePlausibility };

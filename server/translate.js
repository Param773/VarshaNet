// Text translation for non-English social-media posts, so admin-facing
// reports are always readable in English regardless of what language the
// original post was written in. Reuses the exact same ANTHROPIC_API_KEY /
// graceful-degradation pattern server/imageAuthenticity.js already
// established for the photo-plausibility check — same model, same
// timeout/abort handling, same "return null on any failure, caller decides
// what null means" contract.
//
// If ANTHROPIC_API_KEY isn't set, or the call fails, times out, or comes
// back empty, translateToEnglish() resolves to null. Callers must NOT fall
// back to publishing the untranslated original when this is null —
// server/mastodonIngest.js skips the post instead, since showing an admin
// unreadable non-English text defeats the entire point of this module
// existing. A citizen's own report submission is never routed through
// this — it only applies to auto-ingested social posts.

const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY;
const MODEL = "claude-sonnet-4-6";
const TIMEOUT_MS = 15000;

/**
 * @param {string} text - original-language post text (already HTML-stripped)
 * @returns {Promise<string | null>} English translation, or null if
 *   unavailable/failed. Returns the input unchanged (still resolved, not
 *   null) when the model judges it already English — callers that already
 *   know the text is English via isEnglishOnly() shouldn't bother calling
 *   this at all, to save the API call.
 */
async function translateToEnglish(text) {
  if (!ANTHROPIC_API_KEY || !text || !text.trim()) return null;

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), TIMEOUT_MS);

  try {
    const prompt =
      `Translate the following social-media post into natural, concise English. ` +
      `Reply with ONLY the translation — no preamble, no quotes around it, no ` +
      `explanation of what language it was in. If it's already entirely in ` +
      `English, just return it unchanged.\n\n${text}`;

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
        max_tokens: 300,
        messages: [{ role: "user", content: prompt }],
      }),
    });

    if (!res.ok) {
      console.error(`Translation failed: HTTP ${res.status}`);
      return null;
    }

    const data = await res.json();
    const textBlock = (data.content || []).find((c) => c.type === "text");
    if (!textBlock || !textBlock.text.trim()) return null;
    return textBlock.text.trim();
  } catch (e) {
    console.error("Translation failed:", e.message);
    return null;
  } finally {
    clearTimeout(timeout);
  }
}

module.exports = { translateToEnglish };

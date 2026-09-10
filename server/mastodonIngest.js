// Real social-media ingestion pipeline — second live platform, alongside
// blueskyIngest.js's Bluesky adapter (originally Reddit's, before Reddit
// closed free access platform-wide in May 2026 — see blueskyIngest.js's
// header for that history).
//
// Why Mastodon: it's a genuinely free, keyless, *hashtag-based* public API,
// distinct in kind from Bluesky's keyword search — Twitter/X locked search
// behind a paid tier entirely. Mastodon's tag-timeline endpoint —
// GET /api/v1/timelines/tag/:hashtag — is documented as not requiring
// authentication, and this was confirmed live against mastodon.social
// while building this file. It's arguably an even closer match to the PS's
// literal "#IMD and other relevant weather hashtags" wording than either
// Reddit or Bluesky was, since it's a real hashtag-timeline API, not
// keyword search over post text.
//
// Caveat, stated plainly: Mastodon's Indian-weather-topic userbase is much
// smaller than Twitter's ever was, so this adapter will usually create
// fewer reports per run than the keyword-search adapters — that's a genuine
// reach limitation of the platform, not a bug here.
//
// A given instance's admin can disable unauthenticated timeline access
// (Mastodon calls this "authorized fetch" / "limited federation" mode), so
// this tries a short list of large, high-uptime general-purpose instances
// per hashtag and moves to the next one on a non-2xx/network failure
// instead of assuming a single instance is always reachable.
//
// Written the same way blueskyIngest.js is: swap only the fetch layer at
// the top of the pipeline (here, fetchMastodonTag()) and everything from
// `category = detectCategory(...)` onward — dedup, publishing onto Kafka —
// stays untouched. Reports are tagged source: "Social Media (Mastodon)",
// distinct from Bluesky's and from the historical "X / Twitter" seed label.

const { publishRawReport } = require("./reportProducer");
const { detectCategory } = require("./scoring");
const { BoundedSet } = require("./boundedSet");
const { DEFAULT_LOCATION, detectCity, extractHashtags } = require("./socialShared");

// Mastodon hashtags can't contain spaces, so these are single tokens rather
// than the free-text queries blueskyIngest.js uses. Mix of generic weather
// tags, city+rains compound tags common on Indian social media (carried
// over from Twitter-era convention), and single-word rain/flood tags in
// the same nine major Indian languages blueskyIngest.js's SEARCH_QUERIES
// and scoring.js's CATEGORY_KEYWORDS cover — see the comment on
// CATEGORY_KEYWORDS in scoring.js for what this multi-language coverage
// does and doesn't mean.
const HASHTAGS = [
  "rain",
  "rains",
  "monsoon",
  "flood",
  "floods",
  "flooding",
  "heatwave",
  "duststorm",
  "cyclone",
  "imd",
  "mumbairains",
  "delhirains",
  "chennairains",
  "bengalururains",
  "बारिश", // Hindi: rain
  "बाढ़", // Hindi: flood
  "বৃষ্টি", // Bengali: rain
  "বন্যা", // Bengali: flood
  "पाऊस", // Marathi: rain
  "पूर", // Marathi: flood
  "மழை", // Tamil: rain
  "வெள்ளம்", // Tamil: flood
  "వర్షం", // Telugu: rain
  "వరద", // Telugu: flood
  "ಮಳೆ", // Kannada: rain
  "ಪ್ರವಾಹ", // Kannada: flood
  "മഴ", // Malayalam: rain
  "വെള്ളപ്പൊക്കം", // Malayalam: flood
  "વરસાદ", // Gujarati: rain
  "પૂર", // Gujarati: flood
  "ਮੀਂਹ", // Punjabi: rain
  "ਹੜ੍ਹ", // Punjabi: flood
  "بارش", // Urdu: rain
  "سیلاب", // Urdu: flood
];

// Large, high-uptime, general-purpose instances — checked in order per
// hashtag; the next one is only tried if the current one fails outright
// (network error, 4xx/5xx), not just because it returned zero posts.
const INSTANCES = ["mastodon.social", "mastodon.online", "mstdn.social"];

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// Same retry-with-backoff shape as blueskyIngest.js's fetch and
// weather.js's fetchWithRetry — public Mastodon instances are generous
// (~300 req/5min) but can still 429 under bursty conditions.
const MAX_RETRIES = 2;
const RETRY_BASE_DELAY_MS = 1500;

async function fetchMastodonTag(instance, tag, attempt = 0) {
  const url = `https://${instance}/api/v1/timelines/tag/${encodeURIComponent(tag)}?limit=20`;
  const res = await fetch(url, {
    headers: { "User-Agent": "VarshaNet/1.0 (SIH 2026 hackathon project)" },
  });
  if (res.status === 429 && attempt < MAX_RETRIES) {
    await sleep(RETRY_BASE_DELAY_MS * Math.pow(2, attempt));
    return fetchMastodonTag(instance, tag, attempt + 1);
  }
  if (!res.ok) {
    throw new Error(`${instance} tag timeline failed for #${tag} (HTTP ${res.status})`);
  }
  return res.json();
}

// Tries each instance in order for one hashtag, returning the first
// successful response's statuses (or [] if every instance failed).
async function fetchTagWithFallback(tag) {
  for (const instance of INSTANCES) {
    try {
      const statuses = await fetchMastodonTag(instance, tag);
      return Array.isArray(statuses) ? statuses : [];
    } catch (e) {
      console.error(`Mastodon fetch failed on ${instance} for #${tag}:`, e.message);
      // try the next instance
    }
  }
  return [];
}

// Mastodon's `content` field is an HTML fragment (e.g. "<p>Heavy rain in
// Mumbai right now <a href=\"...\">#MumbaiRains</a></p>"). Dependency-free
// strip down to plain text — good enough for this feed's simple markup,
// same "minimal, dependency-free parser" approach imdCapIngest.js takes
// with its RSS. Hashtag links render with the "#" kept as visible text, so
// extractHashtags() still finds them correctly after stripping.
function stripHtml(html) {
  if (!html) return "";
  return html
    .replace(/<br\s*\/?>/gi, " ")
    .replace(/<\/p>/gi, " ")
    .replace(/<[^>]+>/g, "")
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/\s+/g, " ")
    .trim();
}

// Avoid re-creating a report for a status already ingested. A status's
// numeric `id` is only unique per-instance, so the key includes the
// instance host — otherwise two different instances handing back the same
// numeric id could collide. Bounded so this doesn't grow forever over the
// process's lifetime, same pattern as the other ingestion jobs.
const seenStatusKeys = new BoundedSet(5000);

async function ingestTag(tag) {
  const created = [];
  const statuses = await fetchTagWithFallback(tag);

  for (const post of statuses) {
    const instanceHost = post.url ? new URL(post.url).host : "unknown";
    const key = `${instanceHost}:${post.id}`;
    if (!post.id || seenStatusKeys.has(key)) continue;
    seenStatusKeys.add(key); // mark seen even if skipped below, so we don't re-check it every run

    const plainText = stripHtml(post.content);
    if (!plainText) continue;

    // Only ever create a report when a real weather-category keyword is
    // confidently found — same conservative rule every other ingestion
    // job in this project uses, so an off-topic post never becomes a
    // mis-categorized "weather report".
    const category = detectCategory(plainText);
    if (!category) continue;

    const hashtags = extractHashtags(plainText);
    const cityHint = detectCity(plainText) || DEFAULT_LOCATION;
    const description =
      plainText.length > 300 ? plainText.slice(0, 300) + "\u2026" : plainText;

    const mediaAttachments = Array.isArray(post.media_attachments)
      ? post.media_attachments
      : [];
    const hasPhoto = mediaAttachments.some((m) => m.type === "image");
    const hasVideo = mediaAttachments.some((m) => m.type === "video" || m.type === "gifv");
    // Mastodon gives both the original file (`url` — the actual photo, or
    // the actual video/gifv file) and a `preview_url`, which is always a
    // static JPEG frame, generated by Mastodon even for video/gifv
    // attachments. mediaUrl is what a click should open (the real file);
    // mediaThumbUrl is always safe to render as an <img> — using a video
    // file's own URL as an <img> src would just show a broken-image icon.
    const firstMedia = mediaAttachments[0] || null;
    const mediaUrl = firstMedia ? firstMedia.url || firstMedia.remote_url || null : null;
    const mediaThumbUrl = firstMedia ? firstMedia.preview_url || mediaUrl : null;

    // Scoring + the MongoDB write happen in server/worker.js's consumer
    // group now — this pipeline just publishes the candidate.
    const queued = await publishRawReport({
      city: cityHint.name,
      state: cityHint.state,
      lat: cityHint.lat,
      lng: cityHint.lng,
      event: category,
      source: "Social Media (Mastodon)",
      sourceUrl: post.url || null,
      hashtags,
      ts: post.created_at ? Date.parse(post.created_at) : Date.now(),
      text: description,
      hasPhoto,
      hasVideo,
      mediaUrl,
      mediaThumbUrl,
      officialMain: null,
    });

    created.push(queued);
  }

  return created;
}

async function runMastodonIngestion() {
  const created = [];
  for (const tag of HASHTAGS) {
    try {
      const reports = await ingestTag(tag);
      created.push(...reports);
    } catch (e) {
      console.error(`Mastodon ingestion failed for #${tag}:`, e.message);
    }
    await sleep(600); // stay well under public instances' unauthenticated rate limit
  }
  if (created.length) {
    console.log(`Mastodon ingestion: created ${created.length} report(s) from public hashtag timelines.`);
  }
  return created;
}

module.exports = { runMastodonIngestion, HASHTAGS, INSTANCES };

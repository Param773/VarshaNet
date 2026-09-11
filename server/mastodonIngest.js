// Real social-media ingestion pipeline — the project's live social-media
// source. A Bluesky adapter (originally a Reddit adapter before Reddit
// closed free access platform-wide in May 2026) ran alongside this one
// until Bluesky also closed unauthenticated post search; it's since been
// removed from the project. This file was itself removed for one day
// (10 Sep 2026) and reinstated the same day with the relevance fix below,
// then tightened further the same day for two more admin-facing gaps: a
// 7-day recency window (isRecentEnough() below) after old (multi-year-old)
// posts were slipping through, and — after briefly rejecting non-English
// posts outright — a translation step instead (needsTranslation() below,
// via server/translate.js) so a post written in a regional language still
// becomes a report, just with an English-translated description, rather
// than being dropped.
//
// Why Mastodon: it's a genuinely free, keyless, *hashtag-based* public API,
// distinct in kind from keyword search over post text — Twitter/X locked
// search behind a paid tier entirely. Mastodon's tag-timeline endpoint —
// GET /api/v1/timelines/tag/:hashtag — is documented as not requiring
// authentication, and this was confirmed live against mastodon.social
// while building this file. It's arguably an even closer match to the PS's
// literal "#IMD and other relevant weather hashtags" wording than a
// keyword-search API would be, since it's a real hashtag-timeline API.
//
// Caveat, stated plainly: Mastodon's Indian-weather-topic userbase is much
// smaller than Twitter's ever was, so this adapter will usually create
// fewer reports per run than the keyword-search adapters — that's a genuine
// reach limitation of the platform, not a bug here.
//
// What WAS a real bug (fixed in this version): detectCategory() below only
// ever checked whether a post used a weather-category *keyword* — it never
// checked whether the post had anything to do with India. Since generic
// tags like #rain/#flood/#monsoon are used worldwide, any global post using
// one of them (e.g. someone in Brazil posting "#rain again today") passed
// the category check and — because detectCity() found no Indian city in
// it — silently fell back to DEFAULT_LOCATION (New Delhi) and was stored as
// an Indian report. That's the actual relevance gap: not "no filter", but
// "a category filter with no India check behind the DEFAULT_LOCATION
// fallback". isIndiaRelevant() below closes that gap: a post with no
// detected Indian city must now show some other explicit India signal
// (an India/Bharat mention, an Indic script, or an India-specific hashtag
// such as #imd or a city-compound tag) before it's allowed to use the
// DEFAULT_LOCATION fallback. No city + no India signal = dropped.
//
// On India-specific instances: I looked for a well-known, high-uptime,
// general-purpose Mastodon instance run out of India to try ahead of the
// global ones (the way #mumbairains etc. are India-specific hashtags), but
// couldn't find one with the kind of documented uptime the three instances
// below have — hardcoding an obscure/unverified instance domain here would
// just be a new way for this adapter to silently break. So relevance is
// enforced by content (isIndiaRelevant), not by instance choice; INSTANCES
// stays the same large/reliable general-purpose fallback chain as before.
// If a specific India instance is ever confirmed reliable, add its host to
// the front of INSTANCES — nothing else needs to change.
//
// A given instance's admin can disable unauthenticated timeline access
// (Mastodon calls this "authorized fetch" / "limited federation" mode), so
// this tries a short list of large, high-uptime general-purpose instances
// per hashtag and moves to the next one on a non-2xx/network failure
// instead of assuming a single instance is always reachable.
//
// Written the same way blueskyIngest.js was: swap only the fetch layer at
// the top of the pipeline (here, fetchMastodonTag()) and everything from
// `category = detectCategory(...)` onward — dedup, publishing onto Kafka —
// stays untouched. Reports are tagged source: "Social Media (Mastodon)",
// distinct from Bluesky's (now retired) and from the historical
// "X / Twitter" seed label.

const { publishRawReport } = require("./reportProducer");
const { detectCategory } = require("./scoring");
const { BoundedSet } = require("./boundedSet");
const { DEFAULT_LOCATION, detectCity, extractHashtags } = require("./socialShared");
const { translateToEnglish } = require("./translate");

// Mastodon hashtags can't contain spaces, so these are single tokens rather
// than the free-text queries blueskyIngest.js used. Covers all seven
// weather categories server/scoring.js's detectCategory() actually
// recognises (rainfall, thunderstorm, flooding, heatwave, fog, dust_storm,
// strong_winds) — not just rain/flood — plus "imd" itself, city+event
// compound tags common on Indian social media (these double as an India
// signal, see INDIA_HASHTAG_HINTS below), and single-word tags in the same
// nine major Indian languages scoring.js's CATEGORY_KEYWORDS covers. A
// hashtag only decides which posts get *fetched* — detectCategory() still
// runs on each post's full text afterward, so adding a hashtag here never
// bypasses the category or India-relevance gates below. Includes both
// English and regional-language tags: a non-English match isn't dropped
// for being non-English anymore (see the translation step in ingestTag()
// below) — it's translated instead, so the language-specific tags are
// what actually surface those posts in the first place.
const HASHTAGS = [
  // India-specific by construction (see INDIA_HASHTAG_HINTS)
  "imd",
  "mumbairains",
  "delhirains",
  "chennairains",
  "bengalururains",
  "mumbaiweather",
  "delhiweather",
  // rainfall
  "rain",
  "rains",
  "monsoon",
  "drizzle",
  "downpour",
  // thunderstorm
  "thunderstorm",
  "thunder",
  "lightning",
  "hailstorm",
  // flooding
  "flood",
  "floods",
  "flooding",
  "waterlogging",
  // heatwave
  "heatwave",
  "heatstroke",
  // fog
  "fog",
  "smog",
  // dust_storm
  "duststorm",
  "sandstorm",
  // strong_winds (cyclone included here — usually reported with wind/storm
  // language even though it isn't its own scoring.js category)
  "strongwinds",
  "windstorm",
  "cyclone",
  "gale",
  // Non-English hashtags — no longer dropped for being non-English, since
  // translate.js now converts a matching post's text to English instead of
  // the post just getting discarded. These are what actually surface
  // posts written in a regional language in the first place.
  // Hindi
  "बारिश", // rain
  "बाढ़", // flood
  "आंधी", // dust storm
  "बिजली", // lightning
  "कोहरा", // fog
  "लू", // heatwave
  // Bengali
  "বৃষ্টি", // rain
  "বন্যা", // flood
  // Marathi
  "पाऊस", // rain
  "पूर", // flood
  // Tamil
  "மழை", // rain
  "வெள்ளம்", // flood
  // Telugu
  "వర్షం", // rain
  "వరద", // flood
  // Kannada
  "ಮಳೆ", // rain
  "ಪ್ರವಾಹ", // flood
  // Malayalam
  "മഴ", // rain
  "വെള്ളപ്പൊക്കം", // flood
  // Gujarati
  "વરસાદ", // rain
  "પૂર", // flood
  // Punjabi
  "ਮੀਂਹ", // rain
  "ਹੜ੍ਹ", // flood
  // Urdu
  "بارش", // rain
  "سیلاب", // flood
];

// Hashtags that are India-specific by construction (a city-compound tag, or
// "imd" itself) — used by isIndiaRelevant() as a standalone India signal
// even when the post body itself doesn't repeat the country/city name.
const INDIA_HASHTAG_HINTS = new Set([
  "imd",
  "mumbairains",
  "delhirains",
  "chennairains",
  "bengalururains",
  "mumbaiweather",
  "delhiweather",
]);

// Explicit country-name mention. Word-boundary + case-insensitive so it
// doesn't false-positive inside an unrelated longer word.
const INDIA_NAME_REGEX = /\b(india|bharat)\b/i;

// Same Indic-script ranges extractHashtags() in socialShared.js scans for
// (Devanagari, Bengali/Assamese, Gurmukhi, Gujarati, Tamil, Telugu,
// Kannada, Malayalam, Arabic script for Urdu). Used two ways below:
// isIndiaRelevant() treats a match as a standalone India signal (a post
// written in, say, Tamil is overwhelmingly likely to be India-relevant
// even without naming a city), and needsTranslation() uses the same
// regex to decide whether a post's stored description needs to go through
// translate.js before it's published — non-English text is translated
// now instead of being rejected outright.
const INDIC_SCRIPT_REGEX =
  /[\u0600-\u06FF\u0900-\u097F\u0980-\u09FF\u0A00-\u0A7F\u0A80-\u0AFF\u0B80-\u0BFF\u0C00-\u0C7F\u0C80-\u0CFF\u0D00-\u0D7F]/;

function needsTranslation(plainText) {
  return INDIC_SCRIPT_REGEX.test(plainText);
}

// How old a post is allowed to be. Mastodon's tag-timeline endpoint has no
// server-side date filter, so this is enforced here after fetching: a
// hashtag with little recent traffic can still hand back older posts
// within its `limit=20` window, and those need to be dropped explicitly
// rather than assumed recent just because they showed up in a "latest"
// endpoint.
const MAX_POST_AGE_MS = 7 * 24 * 60 * 60 * 1000; // 7 days

function isRecentEnough(post) {
  if (!post.created_at) return false; // no timestamp = can't verify recency, so don't risk it
  const postTime = Date.parse(post.created_at);
  if (Number.isNaN(postTime)) return false;
  return Date.now() - postTime <= MAX_POST_AGE_MS;
}

// True if the post gives some explicit reason to believe it's about India,
// independent of whether detectCity() matched a specific city. Called only
// when detectCity() already came back empty — a matched city is always
// enough on its own.
function isIndiaRelevant(plainText, hashtags) {
  if (INDIA_NAME_REGEX.test(plainText)) return true;
  if (INDIC_SCRIPT_REGEX.test(plainText)) return true;
  return hashtags.some((h) => INDIA_HASHTAG_HINTS.has(h.replace(/^#/, "")));
}

// Large, high-uptime, general-purpose instances — checked in order per
// hashtag; the next one is only tried if the current one fails outright
// (network error, 4xx/5xx), not just because it returned zero posts.
const INSTANCES = ["mastodon.social", "mastodon.online", "mstdn.social"];

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// Same retry-with-backoff shape as weather.js's fetchWithRetry — public
// Mastodon instances are generous (~300 req/5min) but can still 429 under
// bursty conditions.
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

    // Drop anything older than a week before doing any other work on it —
    // no point running category/relevance/translation checks on a post
    // that'll be rejected on recency anyway.
    if (!isRecentEnough(post)) continue;

    // Only ever create a report when a real weather-category keyword is
    // confidently found — same conservative rule every other ingestion
    // job in this project uses, so an off-topic post never becomes a
    // mis-categorized "weather report". Runs on the ORIGINAL-language
    // text: CATEGORY_KEYWORDS already covers the same nine languages, so
    // this doesn't need translation to work correctly.
    const category = detectCategory(plainText);
    if (!category) continue;

    const hashtags = extractHashtags(plainText);
    const cityHint = detectCity(plainText);

    // The actual relevance gate: a specific Indian city is always enough.
    // Failing that, require some other explicit India signal before this
    // global, worldwide hashtag timeline is allowed to fall back to
    // DEFAULT_LOCATION — otherwise a category match alone (e.g. any
    // non-Indian "#rain" post) would silently become a fake New Delhi
    // report. No city + no India signal = drop it here, before it's ever
    // published.
    if (!cityHint && !isIndiaRelevant(plainText, hashtags)) continue;

    const location = cityHint || DEFAULT_LOCATION;

    // Admin-facing text is always English. Posts already in English skip
    // the translation call entirely; a non-English post is translated,
    // and — since showing an admin unreadable text defeats the point —
    // skipped outright (not published with the original text) if
    // translation isn't available (no ANTHROPIC_API_KEY set) or the call
    // fails for any reason. This runs after every other filter so a post
    // that was going to be dropped anyway never costs an API call.
    let englishText = plainText;
    if (needsTranslation(plainText)) {
      const translated = await translateToEnglish(plainText);
      if (!translated) continue;
      englishText = translated;
    }

    const description =
      englishText.length > 300 ? englishText.slice(0, 300) + "\u2026" : englishText;

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
      city: location.name,
      state: location.state,
      lat: location.lat,
      lng: location.lng,
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

module.exports = {
  runMastodonIngestion,
  HASHTAGS,
  INSTANCES,
  isIndiaRelevant,
  needsTranslation,
  isRecentEnough,
};

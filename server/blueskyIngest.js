// Real social-media ingestion pipeline — replaces socialIngest.js's Reddit
// adapter, which stopped being viable when Reddit closed free unauthenticated
// .json access platform-wide on 28-30 May 2026 (see git history / README.md
// for that whole story — the old file is gone, not just disabled, since
// there's no free path back for it).
//
// Why Bluesky: it's a free, keyless, *keyword search* API — the same shape
// of source Reddit was (query text, not just hashtags), which matters
// because it lets this pipeline keep using the same broad
// English-plus-nine-Indian-language query list Reddit used, instead of being
// limited to hashtag-only matching the way mastodonIngest.js is.
//
// The catch, stated plainly: Bluesky's public AppView actually has *two*
// hostnames serving the same read API — public.api.bsky.app and
// api.bsky.app — and only one of them still allows unauthenticated post
// search. Bluesky closed unauthenticated search on public.api.bsky.app in
// June 2026 (same scraper-lockdown wave that hit Reddit and, before that,
// Twitter/X). But api.bsky.app's identical searchPosts endpoint keeps
// working without a login token — confirmed live while building this, and
// corroborated by multiple independent reports through August 2026. Bluesky
// hasn't documented this as an intentional, permanent policy the way Reddit
// did; it may just be an oversight in how the lockdown was rolled out. So
// this pipeline hits api.bsky.app, but detects a platform-wide 403 the same
// defensive way socialIngest.js did for Reddit, in case that door also
// eventually closes — see noteBlueskyDown()/noteBlueskyRecovered() below.
//
// Written the same way the other adapters are: swap only the fetch layer at
// the top (fetchBlueskySearch()) and everything from `category =
// detectCategory(...)` onward — dedup, publishing onto Kafka — stays
// untouched. Reports are tagged source: "Social Media (Bluesky)", distinct
// from Mastodon's and from the historical "X / Twitter" seed label.
//
// mastodonIngest.js is a fully independent pipeline hitting a different
// platform and is unaffected by any of this.

const { publishRawReport } = require("./reportProducer");
const { detectCategory } = require("./scoring");
const { BoundedSet } = require("./boundedSet");
const { DEFAULT_LOCATION, detectCity, extractHashtags } = require("./socialShared");

// Same query list socialIngest.js used for Reddit: English plus the same
// nine major Indian languages CATEGORY_KEYWORDS covers in scoring.js — rain
// and flood in each, since those are the two categories citizens report
// most and the ones most worth searching for even when Bluesky's
// Indian-language-post volume for a given language is thin. See scoring.js's
// CATEGORY_KEYWORDS comment for what "multi-language" does and doesn't mean
// here (keyword matching, not language detection or full NLP).
const SEARCH_QUERIES = [
  "IMD rain India",
  "IMD flood alert",
  "Mumbai rains",
  "Delhi flooding",
  "Chennai rain today",
  "Bengaluru flood",
  "heatwave India",
  "dust storm India",
  "बारिश अलर्ट", // Hindi: rain alert
  "बाढ़ भारत", // Hindi: flood India
  "বৃষ্টি সতর্কতা", // Bengali: rain alert
  "বন্যা ভারত", // Bengali: flood India
  "पाऊस इशारा", // Marathi: rain warning
  "पूर भारत", // Marathi: flood India
  "மழை எச்சரிக்கை", // Tamil: rain alert
  "வெள்ளம் இந்தியா", // Tamil: flood India
  "వర్షం హెచ్చరిక", // Telugu: rain alert
  "వరద భారత్", // Telugu: flood India
  "ಮಳೆ ಎಚ್ಚರಿಕೆ", // Kannada: rain alert
  "ಪ್ರವಾಹ ಭಾರತ", // Kannada: flood India
  "മഴ മുന്നറിയിപ്പ്", // Malayalam: rain alert
  "വെള്ളപ്പൊക്കം ഇന്ത്യ", // Malayalam: flood India
  "વરસાદ ચેતવણી", // Gujarati: rain alert
  "પૂર ભારત", // Gujarati: flood India
  "ਮੀਂਹ ਚੇਤਾਵਨੀ", // Punjabi: rain alert
  "ਹੜ੍ਹ ਭਾਰਤ", // Punjabi: flood India
  "بارش الرٹ", // Urdu: rain alert
  "سیلاب بھارت", // Urdu: flood India
];

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// Same retry-with-backoff shape as the other adapters' fetch helpers —
// public, unauthenticated endpoints are generous but can still 429 under
// bursty/shared-IP conditions.
const MAX_RETRIES = 2;
const RETRY_BASE_DELAY_MS = 1500;

// NOTE: api.bsky.app, not public.api.bsky.app — see file header. Both serve
// the same app.bsky.* Lexicon, but only this host still allows unauthenticated
// searchPosts as of when this was written.
const BSKY_HOST = "https://api.bsky.app";

async function fetchBlueskySearch(query, attempt = 0) {
  const url =
    `${BSKY_HOST}/xrpc/app.bsky.feed.searchPosts?q=` +
    encodeURIComponent(query) +
    "&sort=latest&limit=25";
  const res = await fetch(url, {
    headers: { "User-Agent": "VarshaNet/1.0 (SIH 2026 hackathon project)" },
  });
  if (res.status === 429 && attempt < MAX_RETRIES) {
    await sleep(RETRY_BASE_DELAY_MS * Math.pow(2, attempt));
    return fetchBlueskySearch(query, attempt + 1);
  }
  if (!res.ok) {
    throw new Error(`Bluesky search failed for "${query}" (HTTP ${res.status})`);
  }
  const json = await res.json();
  return Array.isArray(json.posts) ? json.posts : [];
}

// Avoid re-creating a report for a post already ingested. A post's `uri`
// (at://did/collection/rkey) is globally unique, unlike Mastodon's per-
// instance numeric ids, so no extra namespacing is needed here. Bounded so
// this doesn't grow forever over the process's lifetime, same pattern as
// the other ingestion jobs' seen-id sets.
const seenPostUris = new BoundedSet(5000);

// --- Defensive handling for a platform-wide lockout ----------------------
// Same shape as socialIngest.js's Reddit handling: if every query in a
// cycle comes back HTTP 403, that's almost certainly api.bsky.app closing
// the same door public.api.bsky.app already closed, not a fluke on one
// query. Log it once, then fall back to a single cheap probe per cycle
// instead of spending all 28 queries on a door known to be locked — cheap
// enough to notice immediately if access reopens.
let blueskyKnownDown = false;
let lastDownNoticeAt = 0;
const DOWN_NOTICE_REPEAT_MS = 6 * 60 * 60 * 1000; // re-remind at most every 6h

function noteBlueskyDown() {
  const now = Date.now();
  if (!blueskyKnownDown) {
    blueskyKnownDown = true;
    lastDownNoticeAt = now;
    console.warn(
      "Social ingestion (Bluesky): every query returned HTTP 403 — api.bsky.app's " +
        "unauthenticated searchPosts appears to have closed the same way " +
        "public.api.bsky.app's did in June 2026. Switching to a single probe query " +
        "per cycle instead of all " +
        SEARCH_QUERIES.length +
        "; Mastodon ingestion is unaffected and still live."
    );
  } else if (now - lastDownNoticeAt >= DOWN_NOTICE_REPEAT_MS) {
    lastDownNoticeAt = now;
    console.warn("Social ingestion (Bluesky): still returning HTTP 403 (known access issue, not a bug here).");
  }
}

function noteBlueskyRecovered() {
  blueskyKnownDown = false;
  console.log("Social ingestion (Bluesky): a probe query succeeded again — resuming the full query list next cycle.");
}

// Bluesky's post `uri` looks like at://did:plc:xxxx/app.bsky.feed.post/xxxx
// — the last segment is the rkey a bsky.app profile URL needs.
function permalinkFor(post) {
  if (!post.uri || !post.author || !post.author.handle) return null;
  const rkey = post.uri.split("/").pop();
  return `https://bsky.app/profile/${post.author.handle}/post/${rkey}`;
}

// Post-view embeds (resolved media, as opposed to the unresolved reference
// under `record.embed`) come in a few `$type` shapes. Only images and video
// carry a usable thumbnail/click-through; quoted posts and link cards don't
// map to a photo/video the way this pipeline's other sources' media fields
// expect, so they're left as no-media rather than mishandled.
function extractMedia(post) {
  const embed = post.embed;
  if (!embed) return { hasPhoto: false, hasVideo: false, mediaUrl: null, mediaThumbUrl: null };

  if (embed.$type === "app.bsky.embed.images#view" && Array.isArray(embed.images) && embed.images.length) {
    const first = embed.images[0];
    return {
      hasPhoto: true,
      hasVideo: false,
      mediaUrl: first.fullsize || first.thumb || null,
      mediaThumbUrl: first.thumb || first.fullsize || null,
    };
  }

  if (embed.$type === "app.bsky.embed.video#view") {
    // `playlist` is an HLS stream, not something an <img> can render — the
    // generated `thumbnail` frame is the only piece usable as a thumbnail.
    return {
      hasPhoto: false,
      hasVideo: true,
      mediaUrl: embed.playlist || null,
      mediaThumbUrl: embed.thumbnail || null,
    };
  }

  return { hasPhoto: false, hasVideo: false, mediaUrl: null, mediaThumbUrl: null };
}

async function ingestQuery(query, { silent = false } = {}) {
  const created = [];
  try {
    const posts = await fetchBlueskySearch(query);
    if (blueskyKnownDown) noteBlueskyRecovered();
    for (const post of posts) {
      const uri = post.uri;
      if (!uri || seenPostUris.has(uri)) continue;
      seenPostUris.add(uri); // mark seen even if skipped below, so we don't re-check it every run

      const record = post.record || {};
      const text = (record.text || "").trim();
      if (!text) continue;

      // Only ever create a report when a real weather-category keyword is
      // confidently found — same conservative rule every other ingestion
      // job in this project uses, so an off-topic post never becomes a
      // mis-categorized "weather report".
      const category = detectCategory(text);
      if (!category) continue;

      const hashtags = extractHashtags(text);
      const cityHint = detectCity(text) || DEFAULT_LOCATION;
      const description = text.length > 300 ? text.slice(0, 300) + "\u2026" : text;
      const media = extractMedia(post);

      // Scoring + the MongoDB write happen in server/worker.js's consumer
      // group now — this pipeline just publishes the candidate.
      const queued = await publishRawReport({
        city: cityHint.name,
        state: cityHint.state,
        lat: cityHint.lat,
        lng: cityHint.lng,
        event: category,
        source: "Social Media (Bluesky)",
        sourceUrl: permalinkFor(post),
        hashtags,
        ts: record.createdAt ? Date.parse(record.createdAt) : Date.now(),
        text: description,
        hasPhoto: media.hasPhoto,
        hasVideo: media.hasVideo,
        mediaUrl: media.mediaUrl,
        mediaThumbUrl: media.mediaThumbUrl,
        officialMain: null,
      });

      created.push(queued);
    }
  } catch (e) {
    if (/HTTP 403/.test(e.message)) {
      noteBlueskyDown();
    } else if (!silent) {
      console.error(`Social ingestion failed for query "${query}":`, e.message);
    }
  }
  return created;
}

async function runBlueskyIngestion() {
  const created = [];
  // Once we've confirmed Bluesky is down the same way, don't burn all 28
  // queries every cycle on a door that might be locked — one probe is
  // enough to notice if it reopens.
  const queries = blueskyKnownDown ? SEARCH_QUERIES.slice(0, 1) : SEARCH_QUERIES;
  for (const query of queries) {
    const reports = await ingestQuery(query, { silent: blueskyKnownDown });
    created.push(...reports);
    await sleep(600); // stay well under Bluesky's unauthenticated rate limit
  }
  if (created.length) {
    console.log(`Social ingestion: created ${created.length} report(s) from public Bluesky posts.`);
  }
  return created;
}

module.exports = { runBlueskyIngestion, SEARCH_QUERIES };

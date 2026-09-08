// Real social-media ingestion pipeline.
//
// Why Reddit and not Twitter/X: Twitter/X API v2's search endpoint (the one
// that can look up #IMD-style hashtags) has required a paid Basic-tier
// developer plan since 2023 — there is no free, keyless way to search
// live tweets anymore. Rather than fake it (the way the old seed data
// labelled random demo rows "X / Twitter"), this module hits a social
// platform that is genuinely free, keyless, and live: Reddit's public
// read-only search JSON endpoint. It's a real "social media platform"
// per the problem statement, and posts are picked up the same way a
// hashtag-based Twitter listener would — by matching weather keywords
// and hashtag-style tokens in post text.
//
// This is written so a Twitter/X adapter can be dropped in later without
// touching scoring/db code: swap fetchRedditSearch()'s call site for a
// Twitter v2 recent-search call, keep everything from `category =
// detectCategory(...)` onward exactly the same, and reports will keep
// flowing into the same pipeline under a new `source` label.
//
// Reports from here are tagged source: "Social Media (Reddit)" — kept
// distinct from the "X / Twitter" label still used only in historical
// demo/seed data, so the two are never confused as the same kind of proof.
//
// This is no longer the only live social source — mastodonIngest.js hits
// Mastodon's public hashtag-timeline API the same way, so the platform
// name in the problem statement is genuinely plural now, not just Reddit
// relabelled. The two files share city/hashtag-guessing logic via
// socialShared.js but otherwise run as fully independent pipelines.

const db = require("./db");
const { scoreReport, statusFromTrust, detectCategory } = require("./scoring");
const { BoundedSet } = require("./boundedSet");
const { DEFAULT_LOCATION, detectCity, extractHashtags } = require("./socialShared");

// Search queries across English plus the same nine major Indian languages
// CATEGORY_KEYWORDS covers in scoring.js — rain and flood in each, since
// those are the two categories citizens report most and the ones most
// worth searching for even when Reddit's Indian-language-post volume for
// a given language is thin. This replaces what used to be just two
// hardcoded Hindi phrases; see scoring.js's CATEGORY_KEYWORDS comment for
// what "multi-language" does and doesn't mean here (keyword matching,
// not language detection or full NLP).
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

// Reddit's unauthenticated JSON endpoint is real and keyless, but it is
// rate-limit sensitive and occasionally 429s under bursty/shared-IP
// conditions (hackathon wifi, shared hosting) — retry with backoff before
// giving up, same pattern as weather.js's fetchWithRetry.
const MAX_RETRIES = 2;
const RETRY_BASE_DELAY_MS = 1500;

async function fetchRedditSearch(query, attempt = 0) {
  const url =
    "https://www.reddit.com/search.json?q=" +
    encodeURIComponent(query) +
    "&sort=new&limit=10";
  const res = await fetch(url, {
    headers: { "User-Agent": "VarshaNet/1.0 (SIH 2026 hackathon project)" },
  });
  if (res.status === 429 && attempt < MAX_RETRIES) {
    await sleep(RETRY_BASE_DELAY_MS * Math.pow(2, attempt));
    return fetchRedditSearch(query, attempt + 1);
  }
  if (!res.ok) {
    throw new Error(`Reddit search failed for "${query}" (HTTP ${res.status})`);
  }
  const json = await res.json();
  const children = (json.data && json.data.children) || [];
  return children.map((c) => c.data);
}

// Avoid re-creating a report for a post already ingested. Bounded so this
// doesn't grow forever over the process's lifetime, same pattern as the
// other ingestion jobs' seenGuids sets.
const seenPostIds = new BoundedSet(5000);

async function ingestQuery(query) {
  const created = [];
  try {
    const posts = await fetchRedditSearch(query);
    for (const post of posts) {
      const id = post.id || post.name;
      if (!id || seenPostIds.has(id)) continue;
      seenPostIds.add(id); // mark seen even if skipped below, so we don't re-check it every run

      const title = post.title || "";
      const body = post.selftext || "";
      const combinedText = `${title} ${body}`.trim();
      if (!combinedText) continue;

      // Only ever create a report when a real weather-category keyword is
      // confidently found — same conservative rule sachetIngest.js and
      // imdCapIngest.js use, so an off-topic post never becomes a
      // mis-categorized "weather report".
      const category = detectCategory(combinedText);
      if (!category) continue;

      const hashtags = extractHashtags(combinedText);
      const cityHint = detectCity(combinedText) || DEFAULT_LOCATION;
      const description =
        combinedText.length > 300 ? combinedText.slice(0, 300) + "\u2026" : combinedText;
      const hasThumbnail = !!post.thumbnail && post.thumbnail.indexOf("http") === 0;

      const { trustScore } = scoreReport({
        description,
        event: category,
        hasMedia: hasThumbnail,
        mediaReused: false,
        officialMain: null,
        city: cityHint.name,
      });
      const status = statusFromTrust(trustScore);

      const report = await db.addReport({
        city: cityHint.name,
        state: cityHint.state,
        lat: cityHint.lat,
        lng: cityHint.lng,
        event: category,
        autoCategory: category,
        source: "Social Media (Reddit)",
        sourceUrl: post.permalink ? `https://reddit.com${post.permalink}` : null,
        hashtags,
        ts: post.created_utc ? post.created_utc * 1000 : Date.now(),
        trust: trustScore,
        status,
        hasPhoto: hasThumbnail,
        hasVideo: false,
        text: description,
        duplicateOf: null,
        mediaHash: null,
        mediaPath: null,
        perceptualHash: null,
      });

      created.push(report);
    }
  } catch (e) {
    console.error(`Social ingestion failed for query "${query}":`, e.message);
  }
  return created;
}

async function runSocialIngestion() {
  const created = [];
  for (const query of SEARCH_QUERIES) {
    const reports = await ingestQuery(query);
    created.push(...reports);
    await sleep(1200); // stay well under Reddit's unauthenticated rate limit
  }
  if (created.length) {
    console.log(`Social ingestion: created ${created.length} report(s) from public social posts.`);
  }
  return created;
}

module.exports = { runSocialIngestion, SEARCH_QUERIES };

// Shared helpers for the social-media ingestion adapters (socialIngest.js =
// Reddit, mastodonIngest.js = Mastodon). Pulled out so both platforms guess
// city/hashtags the same way instead of two copies quietly drifting apart —
// same approach imdCapIngest.js/sachetIngest.js already take with
// stateLocations.js.

// Used to guess which city a post is about from its text. Checked in order,
// so a post mentioning multiple cities resolves to whichever is listed first.
// `aliases` are the same city name written in a regional script (Hindi plus
// whichever local language is spoken there) — without these, a post entirely
// in, say, Tamil or Bengali would never match its own city and would
// silently fall back to DEFAULT_LOCATION even after detectCategory()
// correctly recognised it as a weather report.
const CITY_HINTS = [
  { name: "Mumbai", state: "Maharashtra", lat: 19.076, lng: 72.8777, aliases: ["मुंबई"] },
  { name: "Delhi", state: "Delhi", lat: 28.7041, lng: 77.1025, aliases: ["दिल्ली"] },
  { name: "Chennai", state: "Tamil Nadu", lat: 13.0827, lng: 80.2707, aliases: ["சென்னை", "चेन्नई"] },
  { name: "Bengaluru", state: "Karnataka", lat: 12.9716, lng: 77.5946, aliases: ["ಬೆಂಗಳೂರು", "बेंगलुरु"] },
  { name: "Bangalore", state: "Karnataka", lat: 12.9716, lng: 77.5946 },
  { name: "Kolkata", state: "West Bengal", lat: 22.5726, lng: 88.3639, aliases: ["কলকাতা", "कोलकाता"] },
  { name: "Hyderabad", state: "Telangana", lat: 17.385, lng: 78.4867, aliases: ["హైదరాబాద్", "हैदराबाद"] },
  { name: "Pune", state: "Maharashtra", lat: 18.5204, lng: 73.8567, aliases: ["पुणे"] },
  { name: "Ahmedabad", state: "Gujarat", lat: 23.0225, lng: 72.5714, aliases: ["અમદાવાદ", "अहमदाबाद"] },
  { name: "Jaipur", state: "Rajasthan", lat: 26.9124, lng: 75.7873, aliases: ["जयपुर"] },
  { name: "Lucknow", state: "Uttar Pradesh", lat: 26.8467, lng: 80.9462, aliases: ["लखनऊ"] },
  { name: "Kochi", state: "Kerala", lat: 9.9312, lng: 76.2673, aliases: ["കൊച്ചി", "कोच्चि"] },
  { name: "Guwahati", state: "Assam", lat: 26.1445, lng: 91.7362, aliases: ["গুৱাহাটী", "गुवाहाटी"] },
];

const DEFAULT_LOCATION = { name: "New Delhi", state: "Delhi", lat: 28.6139, lng: 77.209 };

function detectCity(text) {
  const raw = text || "";
  const lower = raw.toLowerCase();
  return (
    CITY_HINTS.find((c) => {
      if (lower.indexOf(c.name.toLowerCase()) > -1) return true;
      return (c.aliases || []).some((alias) => raw.indexOf(alias) > -1);
    }) || null
  );
}

// Pulls out #hashtag-style tokens so they can be stored alongside the
// report — the literal "#IMD and other relevant weather hashtags" metadata
// the problem statement asks for. Covers Latin plus the Indic scripts used
// by the CATEGORY_KEYWORDS/CITY_HINTS multi-language terms above it:
// Devanagari (Hindi/Marathi), Bengali/Assamese, Gurmukhi (Punjabi),
// Gujarati, Tamil, Telugu, Kannada, Malayalam, and Arabic script (Urdu) —
// so a hashtag written in any of those isn't silently dropped.
function extractHashtags(text) {
  const matches =
    (text || "").match(
      /#[\w\u0600-\u06FF\u0900-\u097F\u0980-\u09FF\u0A00-\u0A7F\u0A80-\u0AFF\u0B80-\u0BFF\u0C00-\u0C7F\u0C80-\u0CFF\u0D00-\u0D7F]+/g
    ) || [];
  return matches.map((h) => h.toLowerCase());
}

module.exports = { CITY_HINTS, DEFAULT_LOCATION, detectCity, extractHashtags };

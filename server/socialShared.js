// Shared helpers for the social-media ingestion adapters (socialIngest.js =
// Reddit, mastodonIngest.js = Mastodon). Pulled out so both platforms guess
// city/hashtags the same way instead of two copies quietly drifting apart —
// same approach imdCapIngest.js/sachetIngest.js already take with
// stateLocations.js.

// Used to guess which city a post is about from its text. Checked in order,
// so a post mentioning multiple cities resolves to whichever is listed first.
const CITY_HINTS = [
  { name: "Mumbai", state: "Maharashtra", lat: 19.076, lng: 72.8777 },
  { name: "Delhi", state: "Delhi", lat: 28.7041, lng: 77.1025 },
  { name: "Chennai", state: "Tamil Nadu", lat: 13.0827, lng: 80.2707 },
  { name: "Bengaluru", state: "Karnataka", lat: 12.9716, lng: 77.5946 },
  { name: "Bangalore", state: "Karnataka", lat: 12.9716, lng: 77.5946 },
  { name: "Kolkata", state: "West Bengal", lat: 22.5726, lng: 88.3639 },
  { name: "Hyderabad", state: "Telangana", lat: 17.385, lng: 78.4867 },
  { name: "Pune", state: "Maharashtra", lat: 18.5204, lng: 73.8567 },
  { name: "Ahmedabad", state: "Gujarat", lat: 23.0225, lng: 72.5714 },
  { name: "Jaipur", state: "Rajasthan", lat: 26.9124, lng: 75.7873 },
  { name: "Lucknow", state: "Uttar Pradesh", lat: 26.8467, lng: 80.9462 },
  { name: "Kochi", state: "Kerala", lat: 9.9312, lng: 76.2673 },
  { name: "Guwahati", state: "Assam", lat: 26.1445, lng: 91.7362 },
];

const DEFAULT_LOCATION = { name: "New Delhi", state: "Delhi", lat: 28.6139, lng: 77.209 };

function detectCity(text) {
  const lower = (text || "").toLowerCase();
  return CITY_HINTS.find((c) => lower.indexOf(c.name.toLowerCase()) > -1) || null;
}

// Pulls out #hashtag-style tokens (Latin + Devanagari) so they can be stored
// alongside the report — the literal "#IMD and other relevant weather
// hashtags" metadata the problem statement asks for.
function extractHashtags(text) {
  const matches = (text || "").match(/#[\w\u0900-\u097F]+/g) || [];
  return matches.map((h) => h.toLowerCase());
}

module.exports = { CITY_HINTS, DEFAULT_LOCATION, detectCity, extractHashtags };

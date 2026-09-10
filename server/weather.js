// Talks to WeatherAPI.com's current-conditions endpoint.
//
// Previously used Open-Meteo (no key, free) — switched away from it because
// Open-Meteo's free tier is rate-limited PER IP ADDRESS, and on Render's
// free tier that IP is shared across many unrelated apps/tenants. The app's
// own call volume was well inside Open-Meteo's documented limits, but every
// request was still coming back 429 from the moment the process booted —
// i.e. the shared IP's quota was already spent by someone else, not by us.
// No amount of our own throttling/caching can fix a quota problem that
// isn't ours to control.
//
// WeatherAPI.com's free tier (1M calls/month, no card required) is
// key-based instead: the quota belongs to this app's account, not to
// whichever IP Render happens to hand out. Set WEATHERAPI_KEY in the
// environment (Render → Environment tab) — get a free key at
// https://www.weatherapi.com/signup.aspx
//
// Bonus: WeatherAPI.com resolves a city name straight to current weather in
// ONE call, so the separate geocoding step Open-Meteo needed is gone
// entirely — half the requests, half the places this could fail.

const WEATHERAPI_KEY = process.env.WEATHERAPI_KEY;
const WEATHERAPI_BASE = "https://api.weatherapi.com/v1/current.json";

function conditionTextToMain(text) {
  const t = (text || "").toLowerCase();
  if (t.includes("thunder")) return "Storm";
  if (t.includes("snow") || t.includes("blizzard") || t.includes("sleet") || t.includes("ice")) return "Snow";
  if (t.includes("fog") || t.includes("mist")) return "Fog";
  if (t.includes("drizzle")) return "Drizzle";
  if (t.includes("rain") || t.includes("shower")) return "Rain";
  if (t.includes("cloud") || t.includes("overcast")) return "Clouds";
  return "Clear";
}

// Kept for compatibility — nothing else in the codebase still calls this
// with an Open-Meteo weather_code, but routes/other modules may import it.
function weatherCodeToMain(code) {
  if ([0, 1].includes(code)) return "Clear";
  if ([2, 3].includes(code)) return "Clouds";
  if ([45, 48].includes(code)) return "Fog";
  if ([51, 53, 55, 56, 57].includes(code)) return "Drizzle";
  if ([61, 63, 65, 66, 67, 80, 81, 82].includes(code)) return "Rain";
  if ([71, 73, 75, 77, 85, 86].includes(code)) return "Snow";
  if ([95, 96, 99].includes(code)) return "Storm";
  return "Clouds";
}

async function safeJson(res) {
  if (!res.ok) throw new Error(`Weather service unavailable right now. (HTTP ${res.status})`);
  try {
    return await res.json();
  } catch (e) {
    throw new Error("Weather service unavailable right now. (bad response body)");
  }
}

const REQUEST_HEADERS = { "User-Agent": "VarshaNet/1.0 (SIH 2026 hackathon project)" };

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// A brief burst can still draw a 429 even on a per-account quota (e.g. the
// per-second rate rather than the monthly cap) — kept as a light safety net,
// not the primary defense anymore now that the quota itself is ours.
const MAX_RETRIES = 3;
const RETRY_BASE_DELAY_MS = 1000;

// --- Circuit breaker for SUSTAINED rate-limiting ------------------------
// If the account key is missing/invalid, or the monthly quota genuinely
// runs out, every call will keep failing — this stops hammering the API
// for a cooldown once that's been true several times in a row, so a sweep
// fails FAST instead of fails SLOW.
const CIRCUIT_TRIP_THRESHOLD = 5;
const CIRCUIT_COOLDOWN_MS = 90 * 1000;
let consecutiveFailures = 0;
let circuitOpenUntil = 0;

function circuitIsOpen() {
  return Date.now() < circuitOpenUntil;
}

function recordSuccess() {
  consecutiveFailures = 0;
}

function recordFailure() {
  consecutiveFailures += 1;
  if (consecutiveFailures >= CIRCUIT_TRIP_THRESHOLD) {
    circuitOpenUntil = Date.now() + CIRCUIT_COOLDOWN_MS;
  }
}

async function fetchWithRetry(url, attempt = 0) {
  if (attempt === 0 && circuitIsOpen()) {
    throw new Error("Weather service unavailable right now. (rate-limited — cooling down, try again shortly)");
  }
  const res = await fetch(url, { headers: REQUEST_HEADERS });
  if (res.status === 429 && attempt < MAX_RETRIES) {
    const delay = RETRY_BASE_DELAY_MS * Math.pow(2, attempt) + Math.random() * 500;
    await sleep(delay);
    return fetchWithRetry(url, attempt + 1);
  }
  if (res.status === 429) {
    recordFailure();
  } else if (res.ok) {
    recordSuccess();
  }
  return res;
}

// --- Global throttle ---------------------------------------------------
// Still worth keeping even on a generous per-account quota: it spaces out
// concurrent callers (ingest sweep, autoResolve sweep, a live user search)
// so they queue instead of racing, and it's what the retry logic above
// assumes is happening.
const MIN_REQUEST_SPACING_MS = 200;
let queueTail = Promise.resolve();

function throttledFetch(url) {
  const result = queueTail.then(() => fetchWithRetry(url));
  queueTail = result.catch(() => {}).then(() => sleep(MIN_REQUEST_SPACING_MS));
  return result;
}

// --- Short-lived weather cache ------------------------------------------
// Multiple callers (ingest.js's sweep, autoResolve.js's re-score, a live
// user search) can ask about the same city within seconds of each other.
// None of that needs a fresh network call every time.
const WEATHER_CACHE_TTL_MS = 4 * 60 * 1000; // 4 minutes
const weatherCache = new Map(); // cityKey -> { data, expiresAt }

function parseWeatherApiResponse(cityLabel, json) {
  const loc = json.location || {};
  const cur = json.current || {};
  return {
    name: loc.name ? loc.name + (loc.region ? ", " + loc.region : "") : cityLabel,
    lat: loc.lat,
    lng: loc.lon,
    temp: Math.round(cur.temp_c),
    humidity: Math.round(cur.humidity),
    wind: Math.round(cur.wind_kph),
    main: conditionTextToMain(cur.condition && cur.condition.text),
  };
}

async function fetchFromWeatherApi(cacheKey, cityLabel, q) {
  const cached = weatherCache.get(cacheKey);
  if (cached && Date.now() < cached.expiresAt) {
    return cached.data;
  }

  if (!WEATHERAPI_KEY) {
    throw new Error(
      "Weather service unavailable right now. (WEATHERAPI_KEY is not set — add it in Render's Environment tab)"
    );
  }

  const url = `${WEATHERAPI_BASE}?key=${encodeURIComponent(WEATHERAPI_KEY)}&q=${encodeURIComponent(q)}`;
  const res = await throttledFetch(url);
  if (res.status === 400) {
    // WeatherAPI's own "location not found" — not a rate-limit issue, so
    // don't count it against the circuit breaker.
    throw new Error("City not found. Try a different spelling.");
  }
  const json = await safeJson(res);
  const result = parseWeatherApiResponse(cityLabel, json);
  weatherCache.set(cacheKey, { data: result, expiresAt: Date.now() + WEATHER_CACHE_TTL_MS });
  return result;
}

async function fetchCityWeather(cityName) {
  const cacheKey = cityName.trim().toLowerCase();
  // WeatherAPI.com's `q` search matches place names worldwide — a bare
  // "Delhi" can resolve to Delhi, Ontario (Canada) instead of Delhi, India.
  // This app is India-only (IMD/SACHET data), so bias the query toward
  // India unless the caller already qualified it (e.g. "Chennai, Tamil Nadu").
  const q = /india/i.test(cityName) ? cityName : `${cityName}, India`;
  return fetchFromWeatherApi(cacheKey, cityName, q);
}

// For callers that already know a city's coordinates (ingest.js's fixed
// WATCH_CITIES list) — uses "lat,lon" as the query, which WeatherAPI.com
// also accepts directly, avoiding any city-name ambiguity.
async function fetchCityWeatherByCoords(cityName, lat, lng) {
  const cacheKey = cityName.trim().toLowerCase();
  return fetchFromWeatherApi(cacheKey, cityName, `${lat},${lng}`);
}

module.exports = { fetchCityWeather, fetchCityWeatherByCoords, weatherCodeToMain };

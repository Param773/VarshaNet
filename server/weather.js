// Talks to Open-Meteo's free geocoding + forecast APIs (no key required).
// Ported from the original client-side fetchCityWeather() so both the
// forecast search page and the report-scoring pipeline share one
// implementation, running server-side.

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

// Open-Meteo's free tier will return 429 under bursty load — this project
// alone can burst 8 cities x 2 calls at once. A 429 isn't a "this city
// failed" situation, it's "slow down", so it's worth a couple of quick
// retries with backoff before giving up on that request. Any other HTTP
// error (or a repeated 429) still fails immediately, same as before.
const MAX_RETRIES = 3;
const RETRY_BASE_DELAY_MS = 1000;

// --- Circuit breaker for SUSTAINED rate-limiting ------------------------
// The retry ladder above is for a brief burst — it assumes the API comes
// back within a few seconds. But Open-Meteo's free tier can also be
// rate-limited for real, for minutes at a stretch (its quota is shared
// globally, not per-request). Without this, a 200-city sweep run during
// an outage would retry every single city's full ladder, fail every
// single time, and turn what should be a quick "nothing worked" into a
// 30+ minute run for zero benefit — which is exactly what "Pull Live
// Data" looked stuck doing. Once several calls in a row exhaust their
// retries and still fail, assume this is a sustained outage rather than
// a burst: stop hitting the network at all for a cooldown, so every
// remaining city in the sweep fails FAST instead of fails SLOW. The very
// next call after the cooldown gets a normal, full-retry attempt — if
// Open-Meteo has recovered by then, this closes on its own.
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
    // Fail instantly, no network call at all — this is what actually
    // saves the time; without it every remaining city would still pay
    // the full multi-second retry ladder below just to fail anyway.
    throw new Error("Weather service unavailable right now. (rate-limited — cooling down, try again shortly)");
  }
  const res = await fetch(url, { headers: REQUEST_HEADERS });
  if (res.status === 429 && attempt < MAX_RETRIES) {
    // Base backoff (1s, 2s, 4s) plus up to 500ms of jitter — without the
    // jitter, every city in the same batch hits a 429 at roughly the same
    // moment and then retries at the exact same moment too, so the retries
    // just collide and re-trigger the rate limit as a group.
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
// This module has more than one caller: the 200-city ingestion sweep
// (ingest.js), the pending-report re-score sweep (autoResolve.js), a
// report's submission-time lookup (routes/reports.js), and a single
// on-demand search from the Forecast page (routes/weather.js). They all
// share the same Open-Meteo free-tier rate limit, so a busy background
// sweep can eat the limit right out from under one person typing a city
// into the search box — that's the "Weather service unavailable (HTTP
// 429)" a real visitor sees, even though nothing is actually wrong with
// their request.
//
// Every outbound call is funneled through this one queue, dispatched a
// fixed minimum interval apart, so no single caller can flood the limit
// and every request — background or user-facing — still gets served,
// just spaced out instead of racing.
const MIN_REQUEST_SPACING_MS = 200;
let queueTail = Promise.resolve();

function throttledFetch(url) {
  const result = queueTail.then(() => fetchWithRetry(url));
  // Chain the next dispatch after this one's spacing delay regardless of
  // outcome, so one failed/slow request never stalls everyone behind it.
  queueTail = result.catch(() => {}).then(() => sleep(MIN_REQUEST_SPACING_MS));
  return result;
}

// A city's lat/lng doesn't change between calls, only its current weather
// does — so geocoding results are cached in memory for the life of the
// process. This means every ingestion run after the first only re-fetches
// weather itself, roughly halving the API calls per cycle.
const geoCache = new Map();

async function geocodeCity(cityName) {
  const key = cityName.trim().toLowerCase();
  if (geoCache.has(key)) return geoCache.get(key);

  const geoUrl =
    "https://geocoding-api.open-meteo.com/v1/search?count=1&name=" + encodeURIComponent(cityName);
  const geoRes = await throttledFetch(geoUrl);
  const geo = await safeJson(geoRes);
  if (!geo.results || !geo.results.length) {
    throw new Error("City not found. Try a different spelling.");
  }
  const loc = geo.results[0];
  geoCache.set(key, loc);
  return loc;
}

async function fetchCityWeather(cityName) {
  const loc = await geocodeCity(cityName);
  const wUrl =
    "https://api.open-meteo.com/v1/forecast?latitude=" +
    loc.latitude +
    "&longitude=" +
    loc.longitude +
    "&current=temperature_2m,relative_humidity_2m,wind_speed_10m,weather_code&timezone=auto";
  const wRes = await throttledFetch(wUrl);
  const w = await safeJson(wRes);
  const cur = w.current || {};
  return {
    name: loc.name + (loc.admin1 ? ", " + loc.admin1 : ""),
    lat: loc.latitude,
    lng: loc.longitude,
    temp: Math.round(cur.temperature_2m),
    humidity: Math.round(cur.relative_humidity_2m),
    wind: Math.round(cur.wind_speed_10m),
    main: weatherCodeToMain(cur.weather_code),
  };
}

module.exports = { fetchCityWeather, weatherCodeToMain };
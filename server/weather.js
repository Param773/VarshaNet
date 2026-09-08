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

async function fetchWithRetry(url, attempt = 0) {
  const res = await fetch(url, { headers: REQUEST_HEADERS });
  if (res.status === 429 && attempt < MAX_RETRIES) {
    const delay = RETRY_BASE_DELAY_MS * Math.pow(2, attempt); // 1s, 2s, 4s
    await sleep(delay);
    return fetchWithRetry(url, attempt + 1);
  }
  return res;
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
  const geoRes = await fetchWithRetry(geoUrl);
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
  const wRes = await fetchWithRetry(wUrl);
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
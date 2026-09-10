// Real public-API ingestion pipeline.
//
// Everything else that looks like "social media" or "public dataset" data
// in this project is historical seed data generated for demo purposes. This
// module is different: it genuinely calls a live, free public weather API
// (Open-Meteo, no key required — the same one server/weather.js already
// uses for scoring) for one representative city per Indian state/UT, and
// whenever a city is *currently* experiencing notable weather, it
// publishes a candidate report onto the varshanet.raw-reports Kafka topic
// (see server/reportProducer.js) for server/worker.js's consumer group to
// score and persist.
//
// Trimmed from an earlier 200-city list down to one city per state/UT
// (33 total): with 200 cities, a full sweep made ~200-400 calls to
// Open-Meteo's free tier every run, which made it easy to trip a
// sustained rate limit (see server/weather.js's circuit breaker) and, even
// on a good run, took several minutes just from the batch pauses below.
// One representative city per state still gives state-level coverage
// (matching the granularity server/stateLocations.js already resolves
// SACHET/IMD alerts to) at a fraction of the API load and run time.

const { fetchCityWeatherByCoords } = require("./weather");
const { publishRawReport } = require("./reportProducer");

// Coordinates are hardcoded (rather than resolved via geocodeCity() at
// runtime) so this fixed 33-city list never needs a geocoding call at all —
// see weather.js's fetchCityWeatherByCoords() comment for why that matters
// on every cold start.
const WATCH_CITIES = [
  { city: "Delhi", state: "Delhi", lat: 28.6139, lng: 77.209 },
  { city: "Mumbai", state: "Maharashtra", lat: 19.076, lng: 72.8777 },
  { city: "Kolkata", state: "West Bengal", lat: 22.5726, lng: 88.3639 },
  { city: "Chennai", state: "Tamil Nadu", lat: 13.0827, lng: 80.2707 },
  { city: "Bengaluru", state: "Karnataka", lat: 12.9716, lng: 77.5946 },
  { city: "Hyderabad", state: "Telangana", lat: 17.385, lng: 78.4867 },
  { city: "Ahmedabad", state: "Gujarat", lat: 23.0225, lng: 72.5714 },
  { city: "Jaipur", state: "Rajasthan", lat: 26.9124, lng: 75.7873 },
  { city: "Lucknow", state: "Uttar Pradesh", lat: 26.8467, lng: 80.9462 },
  { city: "Patna", state: "Bihar", lat: 25.5941, lng: 85.1376 },
  { city: "Bhopal", state: "Madhya Pradesh", lat: 23.2599, lng: 77.4126 },
  { city: "Guwahati", state: "Assam", lat: 26.1445, lng: 91.7362 },
  { city: "Chandigarh", state: "Chandigarh", lat: 30.7333, lng: 76.7794 },
  { city: "Thiruvananthapuram", state: "Kerala", lat: 8.5241, lng: 76.9366 },
  { city: "Bhubaneswar", state: "Odisha", lat: 20.2961, lng: 85.8245 },
  { city: "Ranchi", state: "Jharkhand", lat: 23.3441, lng: 85.3096 },
  { city: "Raipur", state: "Chhattisgarh", lat: 21.2514, lng: 81.6296 },
  { city: "Amritsar", state: "Punjab", lat: 31.634, lng: 74.8723 },
  { city: "Visakhapatnam", state: "Andhra Pradesh", lat: 17.6868, lng: 83.2185 },
  { city: "Dehradun", state: "Uttarakhand", lat: 30.3165, lng: 78.0322 },
  { city: "Shimla", state: "Himachal Pradesh", lat: 31.1048, lng: 77.1734 },
  { city: "Srinagar", state: "Jammu and Kashmir", lat: 34.0837, lng: 74.7973 },
  { city: "Puducherry", state: "Puducherry", lat: 11.9416, lng: 79.8083 },
  { city: "Panaji", state: "Goa", lat: 15.4909, lng: 73.8278 },
  { city: "Faridabad", state: "Haryana", lat: 28.4089, lng: 77.3178 },
  { city: "Imphal", state: "Manipur", lat: 24.817, lng: 93.9368 },
  { city: "Aizawl", state: "Mizoram", lat: 23.7271, lng: 92.7176 },
  { city: "Shillong", state: "Meghalaya", lat: 25.5788, lng: 91.8933 },
  { city: "Agartala", state: "Tripura", lat: 23.8315, lng: 91.2868 },
  { city: "Kohima", state: "Nagaland", lat: 25.6751, lng: 94.1086 },
  { city: "Itanagar", state: "Arunachal Pradesh", lat: 27.0844, lng: 93.6053 },
  { city: "Gangtok", state: "Sikkim", lat: 27.3389, lng: 88.6065 },
  { city: "Port Blair", state: "Andaman and Nicobar Islands", lat: 11.6234, lng: 92.7265 },
];

// Don't re-create a report for the same city+event combo more than once
// every few hours, so a repeated poll doesn't spam the queue while the same
// weather system is still sitting over a city.
const REINGEST_COOLDOWN_MS = 20 * 60 * 1000; // 20 minutes — short enough for demo re-runs
const recentlyIngested = new Map(); // "city:event" -> last-created timestamp

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// Deliberately a little generous — this is scanning for *any* live signal
// worth surfacing, not just extreme/rare events. Heavy rain, gusty wind,
// noticeable heat and reduced-visibility fog are all things IMD itself
// issues routine advisories for.
function detectEventFromWeather(w) {
  if (w.main === "Storm") return "thunderstorm";
  if (w.wind >= 28) return "strong_winds";
  if (w.main === "Rain" || w.main === "Drizzle") return "rainfall";
  if (w.main === "Fog") return "fog";
  if (w.temp >= 37) return "heatwave";
  return null; // genuinely calm right now
}

function describeEvent(event, w, city) {
  switch (event) {
    case "thunderstorm":
      return `Live weather feed shows thunderstorm activity over ${city}.`;
    case "strong_winds":
      return `Sustained winds around ${w.wind} km/h recorded near ${city}.`;
    case "rainfall":
      return `Ongoing rainfall recorded over ${city} by live weather feed.`;
    case "fog":
      return `Reduced visibility due to fog reported near ${city}.`;
    case "heatwave":
      return `Temperature around ${w.temp}\u00B0C recorded in ${city}, heatwave-like conditions.`;
    default:
      return `Notable weather activity in ${city}.`;
  }
}

// Each city resolves to exactly one of these outcomes, so a single ingestion
// run can report a clear breakdown instead of just a raw "created" count.
const OUTCOME = { CREATED: "created", CALM: "calm", COOLDOWN: "cooldown", FAILED: "failed" };

async function ingestCity(entry) {
  try {
    const w = await fetchCityWeatherByCoords(entry.city, entry.lat, entry.lng);
    const event = detectEventFromWeather(w);
    if (!event) return { outcome: OUTCOME.CALM };

    const cooldownKey = entry.city + ":" + event;
    const last = recentlyIngested.get(cooldownKey);
    if (last && Date.now() - last < REINGEST_COOLDOWN_MS) {
      return { outcome: OUTCOME.COOLDOWN };
    }

    const description = describeEvent(event, w, entry.city);

    // Scoring + the MongoDB write both now happen in server/worker.js's
    // consumer group, not here — this pipeline's job is just "found a
    // candidate, hand it to the stream" (see server/reportProducer.js).
    const queued = await publishRawReport({
      city: entry.city,
      state: entry.state,
      lat: w.lat,
      lng: w.lng,
      event,
      source: "Weather API",
      ts: Date.now(),
      text: description,
      hasPhoto: false,
      hasVideo: false,
      officialMain: w.main,
    });

    recentlyIngested.set(cooldownKey, Date.now());
    return { outcome: OUTCOME.CREATED, report: queued };
  } catch (e) {
    console.error(`Ingestion failed for ${entry.city}:`, e.message);
    return { outcome: OUTCOME.FAILED, error: e.message };
  }
}

// Cities are checked in small parallel batches rather than one at a time —
// with 200 cities, a fully sequential pass would take too long for an admin
// sitting there watching the "Pull Live Data Now" button.
//
// On a cold start (empty geoCache) each city needs 2 calls, so a batch of 5
// fires 10 requests at once. Open-Meteo's free tier can't absorb that burst,
// and because every city in the batch retries on the same fixed 1s/2s/4s
// schedule, the retries collide too and the whole batch dies together. A
// smaller batch + longer pause keeps concurrent load low; once geoCache is
// warm (after the first successful cycle) load halves on its own.
const BATCH_SIZE = 3;
const BATCH_PAUSE_MS = 2500;

async function runIngestion() {
  const created = [];
  const tally = { created: 0, calm: 0, cooldown: 0, failed: 0 };

  for (let i = 0; i < WATCH_CITIES.length; i += BATCH_SIZE) {
    const batch = WATCH_CITIES.slice(i, i + BATCH_SIZE);
    const results = await Promise.all(batch.map(ingestCity));
    results.forEach((r) => {
      tally[r.outcome] += 1;
      if (r.outcome === OUTCOME.CREATED) created.push(r.report);
    });
    await sleep(BATCH_PAUSE_MS);
  }

  console.log(
    `Live ingestion summary — checked ${WATCH_CITIES.length} cities: ` +
      `${tally.created} created, ${tally.calm} calm, ${tally.cooldown} on cooldown, ${tally.failed} failed.`
  );
  return created;
}

module.exports = { runIngestion, WATCH_CITIES };
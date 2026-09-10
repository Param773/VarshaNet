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

const { fetchCityWeather } = require("./weather");
const { publishRawReport } = require("./reportProducer");

const WATCH_CITIES = [
  { city: "Delhi", state: "Delhi" },
  { city: "Mumbai", state: "Maharashtra" },
  { city: "Kolkata", state: "West Bengal" },
  { city: "Chennai", state: "Tamil Nadu" },
  { city: "Bengaluru", state: "Karnataka" },
  { city: "Hyderabad", state: "Telangana" },
  { city: "Ahmedabad", state: "Gujarat" },
  { city: "Jaipur", state: "Rajasthan" },
  { city: "Lucknow", state: "Uttar Pradesh" },
  { city: "Patna", state: "Bihar" },
  { city: "Bhopal", state: "Madhya Pradesh" },
  { city: "Guwahati", state: "Assam" },
  { city: "Chandigarh", state: "Chandigarh" },
  { city: "Thiruvananthapuram", state: "Kerala" },
  { city: "Bhubaneswar", state: "Odisha" },
  { city: "Ranchi", state: "Jharkhand" },
  { city: "Raipur", state: "Chhattisgarh" },
  { city: "Amritsar", state: "Punjab" },
  { city: "Visakhapatnam", state: "Andhra Pradesh" },
  { city: "Dehradun", state: "Uttarakhand" },
  { city: "Shimla", state: "Himachal Pradesh" },
  { city: "Srinagar", state: "Jammu and Kashmir" },
  { city: "Puducherry", state: "Puducherry" },
  { city: "Panaji", state: "Goa" },
  { city: "Faridabad", state: "Haryana" },
  { city: "Imphal", state: "Manipur" },
  { city: "Aizawl", state: "Mizoram" },
  { city: "Shillong", state: "Meghalaya" },
  { city: "Agartala", state: "Tripura" },
  { city: "Kohima", state: "Nagaland" },
  { city: "Itanagar", state: "Arunachal Pradesh" },
  { city: "Gangtok", state: "Sikkim" },
  { city: "Port Blair", state: "Andaman and Nicobar Islands" },
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
    const w = await fetchCityWeather(entry.city);
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
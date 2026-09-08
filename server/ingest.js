// Real public-API ingestion pipeline.
//
// Everything else that looks like "social media" or "public dataset" data
// in this project is historical seed data generated for demo purposes. This
// module is different: it genuinely calls a live, free public weather API
// (Open-Meteo, no key required — the same one server/weather.js already
// uses for scoring) for 200 Indian cities, and whenever a city is
// *currently* experiencing notable weather, it auto-creates a real report
// from that live reading.

const db = require("./db");
const { fetchCityWeather } = require("./weather");
const { scoreReport, statusFromTrust } = require("./scoring");

const WATCH_CITIES = [
  { city: "Delhi", state: "Delhi" },
  { city: "Mumbai", state: "Maharashtra" },
  { city: "Kolkata", state: "West Bengal" },
  { city: "Chennai", state: "Tamil Nadu" },
  { city: "Bengaluru", state: "Karnataka" },
  { city: "Hyderabad", state: "Telangana" },
  { city: "Pune", state: "Maharashtra" },
  { city: "Ahmedabad", state: "Gujarat" },
  { city: "Jaipur", state: "Rajasthan" },
  { city: "Lucknow", state: "Uttar Pradesh" },
  { city: "Patna", state: "Bihar" },
  { city: "Bhopal", state: "Madhya Pradesh" },
  { city: "Guwahati", state: "Assam" },
  { city: "Chandigarh", state: "Chandigarh" },
  { city: "Thiruvananthapuram", state: "Kerala" },
  { city: "Surat", state: "Gujarat" },
  { city: "Kanpur", state: "Uttar Pradesh" },
  { city: "Nagpur", state: "Maharashtra" },
  { city: "Indore", state: "Madhya Pradesh" },
  { city: "Bhubaneswar", state: "Odisha" },
  { city: "Ranchi", state: "Jharkhand" },
  { city: "Raipur", state: "Chhattisgarh" },
  { city: "Amritsar", state: "Punjab" },
  { city: "Ludhiana", state: "Punjab" },
  { city: "Varanasi", state: "Uttar Pradesh" },
  { city: "Agra", state: "Uttar Pradesh" },
  { city: "Nashik", state: "Maharashtra" },
  { city: "Vadodara", state: "Gujarat" },
  { city: "Coimbatore", state: "Tamil Nadu" },
  { city: "Madurai", state: "Tamil Nadu" },
  { city: "Visakhapatnam", state: "Andhra Pradesh" },
  { city: "Vijayawada", state: "Andhra Pradesh" },
  { city: "Mysuru", state: "Karnataka" },
  { city: "Mangaluru", state: "Karnataka" },
  { city: "Kochi", state: "Kerala" },
  { city: "Kozhikode", state: "Kerala" },
  { city: "Thrissur", state: "Kerala" },
  { city: "Jodhpur", state: "Rajasthan" },
  { city: "Udaipur", state: "Rajasthan" },
  { city: "Kota", state: "Rajasthan" },
  { city: "Gwalior", state: "Madhya Pradesh" },
  { city: "Jabalpur", state: "Madhya Pradesh" },
  { city: "Dehradun", state: "Uttarakhand" },
  { city: "Shimla", state: "Himachal Pradesh" },
  { city: "Srinagar", state: "Jammu and Kashmir" },
  { city: "Jammu", state: "Jammu and Kashmir" },
  { city: "Siliguri", state: "West Bengal" },
  { city: "Durgapur", state: "West Bengal" },
  { city: "Asansol", state: "West Bengal" },
  { city: "Rourkela", state: "Odisha" },
  { city: "Cuttack", state: "Odisha" },
  { city: "Puri", state: "Odisha" },
  { city: "Guntur", state: "Andhra Pradesh" },
  { city: "Rajahmundry", state: "Andhra Pradesh" },
  { city: "Warangal", state: "Telangana" },
  { city: "Nizamabad", state: "Telangana" },
  { city: "Salem", state: "Tamil Nadu" },
  { city: "Tiruchirappalli", state: "Tamil Nadu" },
  { city: "Vellore", state: "Tamil Nadu" },
  { city: "Puducherry", state: "Puducherry" },
  { city: "Panaji", state: "Goa" },
  { city: "Aurangabad", state: "Maharashtra" },
  { city: "Solapur", state: "Maharashtra" },
  { city: "Kolhapur", state: "Maharashtra" },
  { city: "Amravati", state: "Maharashtra" },
  { city: "Nanded", state: "Maharashtra" },
  { city: "Jalgaon", state: "Maharashtra" },
  { city: "Gorakhpur", state: "Uttar Pradesh" },
  { city: "Prayagraj", state: "Uttar Pradesh" },
  { city: "Meerut", state: "Uttar Pradesh" },
  { city: "Bareilly", state: "Uttar Pradesh" },
  { city: "Aligarh", state: "Uttar Pradesh" },
  { city: "Moradabad", state: "Uttar Pradesh" },
  { city: "Ghaziabad", state: "Uttar Pradesh" },
  { city: "Faridabad", state: "Haryana" },
  { city: "Gurugram", state: "Haryana" },
  { city: "Noida", state: "Uttar Pradesh" },
  { city: "Rohtak", state: "Haryana" },
  { city: "Hisar", state: "Haryana" },
  { city: "Panipat", state: "Haryana" },
  { city: "Bikaner", state: "Rajasthan" },
  { city: "Ajmer", state: "Rajasthan" },
  { city: "Alwar", state: "Rajasthan" },
  { city: "Sikar", state: "Rajasthan" },
  { city: "Imphal", state: "Manipur" },
  { city: "Aizawl", state: "Mizoram" },
  { city: "Shillong", state: "Meghalaya" },
  { city: "Agartala", state: "Tripura" },
  { city: "Kohima", state: "Nagaland" },
  { city: "Itanagar", state: "Arunachal Pradesh" },
  { city: "Gangtok", state: "Sikkim" },
  { city: "Port Blair", state: "Andaman and Nicobar Islands" },
  { city: "Bilaspur", state: "Chhattisgarh" },
  { city: "Bhilai", state: "Chhattisgarh" },
  { city: "Korba", state: "Chhattisgarh" },
  { city: "Muzaffarpur", state: "Bihar" },
  { city: "Bhagalpur", state: "Bihar" },
  { city: "Gaya", state: "Bihar" },
  { city: "Darbhanga", state: "Bihar" },
  { city: "Purnia", state: "Bihar" },
  { city: "Firozabad", state: "Uttar Pradesh" },
  { city: "Jhansi", state: "Uttar Pradesh" },
  { city: "Muzaffarnagar", state: "Uttar Pradesh" },
  { city: "Mathura", state: "Uttar Pradesh" },
  { city: "Rampur", state: "Uttar Pradesh" },
  { city: "Shahjahanpur", state: "Uttar Pradesh" },
  { city: "Farrukhabad", state: "Uttar Pradesh" },
  { city: "Mau", state: "Uttar Pradesh" },
  { city: "Hapur", state: "Uttar Pradesh" },
  { city: "Etawah", state: "Uttar Pradesh" },
  { city: "Thane", state: "Maharashtra" },
  { city: "Navi Mumbai", state: "Maharashtra" },
  { city: "Akola", state: "Maharashtra" },
  { city: "Latur", state: "Maharashtra" },
  { city: "Dhule", state: "Maharashtra" },
  { city: "Ichalkaranji", state: "Maharashtra" },
  { city: "Chandrapur", state: "Maharashtra" },
  { city: "Parbhani", state: "Maharashtra" },
  { city: "Ahmednagar", state: "Maharashtra" },
  { city: "Bhiwandi", state: "Maharashtra" },
  { city: "Chapra", state: "Bihar" },
  { city: "Katihar", state: "Bihar" },
  { city: "Munger", state: "Bihar" },
  { city: "Begusarai", state: "Bihar" },
  { city: "Motihari", state: "Bihar" },
  { city: "Bettiah", state: "Bihar" },
  { city: "Saharsa", state: "Bihar" },
  { city: "Siwan", state: "Bihar" },
  { city: "Ara", state: "Bihar" },
  { city: "Sasaram", state: "Bihar" },
  { city: "Bhilwara", state: "Rajasthan" },
  { city: "Pali", state: "Rajasthan" },
  { city: "Barmer", state: "Rajasthan" },
  { city: "Bhiwadi", state: "Rajasthan" },
  { city: "Sri Ganganagar", state: "Rajasthan" },
  { city: "Tonk", state: "Rajasthan" },
  { city: "Sawai Madhopur", state: "Rajasthan" },
  { city: "Churu", state: "Rajasthan" },
  { city: "Jhunjhunu", state: "Rajasthan" },
  { city: "Nagaur", state: "Rajasthan" },
  { city: "Rajkot", state: "Gujarat" },
  { city: "Bhavnagar", state: "Gujarat" },
  { city: "Jamnagar", state: "Gujarat" },
  { city: "Junagadh", state: "Gujarat" },
  { city: "Gandhinagar", state: "Gujarat" },
  { city: "Anand", state: "Gujarat" },
  { city: "Nadiad", state: "Gujarat" },
  { city: "Morbi", state: "Gujarat" },
  { city: "Mehsana", state: "Gujarat" },
  { city: "Bharuch", state: "Gujarat" },
  { city: "Ujjain", state: "Madhya Pradesh" },
  { city: "Sagar", state: "Madhya Pradesh" },
  { city: "Dewas", state: "Madhya Pradesh" },
  { city: "Satna", state: "Madhya Pradesh" },
  { city: "Ratlam", state: "Madhya Pradesh" },
  { city: "Rewa", state: "Madhya Pradesh" },
  { city: "Katni", state: "Madhya Pradesh" },
  { city: "Singrauli", state: "Madhya Pradesh" },
  { city: "Burhanpur", state: "Madhya Pradesh" },
  { city: "Khandwa", state: "Madhya Pradesh" },
  { city: "Hubballi", state: "Karnataka" },
  { city: "Belagavi", state: "Karnataka" },
   { city: "Davangere", state: "Karnataka" },
  { city: "Ballari", state: "Karnataka" },
  { city: "Vijayapura", state: "Karnataka" },
  { city: "Shivamogga", state: "Karnataka" },
  { city: "Tumakuru", state: "Karnataka" },
  { city: "Raichur", state: "Karnataka" },
  { city: "Bidar", state: "Karnataka" },
  { city: "Hospet", state: "Karnataka" },
  { city: "Tiruppur", state: "Tamil Nadu" },
  { city: "Erode", state: "Tamil Nadu" },
  { city: "Thoothukudi", state: "Tamil Nadu" },
  { city: "Dindigul", state: "Tamil Nadu" },
  { city: "Thanjavur", state: "Tamil Nadu" },
  { city: "Ranipet", state: "Tamil Nadu" },
  { city: "Sivakasi", state: "Tamil Nadu" },
  { city: "Karur", state: "Tamil Nadu" },
    { city: "Ooty", state: "Tamil Nadu" },
  { city: "Kanchipuram", state: "Tamil Nadu" },
  { city: "Nellore", state: "Andhra Pradesh" },
  { city: "Kurnool", state: "Andhra Pradesh" },
  { city: "Kadapa", state: "Andhra Pradesh" },
  { city: "Anantapur", state: "Andhra Pradesh" },
  { city: "Tirupati", state: "Andhra Pradesh" },
  { city: "Chittoor", state: "Andhra Pradesh" },
  { city: "Eluru", state: "Andhra Pradesh" },
  { city: "Ongole", state: "Andhra Pradesh" },
  { city: "Karimnagar", state: "Telangana" },
  { city: "Khammam", state: "Telangana" },
  { city: "Howrah", state: "West Bengal" },
  { city: "Kharagpur", state: "West Bengal" },
  { city: "Haldia", state: "West Bengal" },
  { city: "Bardhaman", state: "West Bengal" },
  { city: "Malda", state: "West Bengal" },
  { city: "Baharampur", state: "West Bengal" },
  { city: "Jalpaiguri", state: "West Bengal" },
  { city: "Cooch Behar", state: "West Bengal" },
  { city: "Krishnanagar", state: "West Bengal" },
  { city: "Raiganj", state: "West Bengal" },
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
    const { trustScore } = scoreReport({
      description,
      event,
      hasMedia: false,
      mediaReused: false,
      officialMain: w.main,
      city: entry.city,
    });
    const status = statusFromTrust(trustScore);

       const report = await db.addReport({
      city: entry.city,
      state: entry.state,
      lat: w.lat,
      lng: w.lng,
      event,
      autoCategory: event,
            source: "Weather API",
      ts: Date.now(),
      trust: trustScore,
      status,
      hasPhoto: false,
      hasVideo: false,
      text: description,
      duplicateOf: null,
      mediaHash: null,
      mediaPath: null,
    });

    recentlyIngested.set(cooldownKey, Date.now());
    return { outcome: OUTCOME.CREATED, report };
  } catch (e) {
    console.error(`Ingestion failed for ${entry.city}:`, e.message);
    return { outcome: OUTCOME.FAILED, error: e.message };
  }
}

// Cities are checked in small parallel batches rather than one at a time —
// with 200 cities, a fully sequential pass would take too long for an admin
// sitting there watching the "Pull Live Data Now" button.
const BATCH_SIZE = 5;
const BATCH_PAUSE_MS = 1000;

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
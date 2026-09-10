// Generates realistic-looking historical demo reports so the dashboard isn't
// empty on first boot. Ported from the original client-side generateReports()
// — same cities, categories and snippets — but it now runs once on the
// server and the results are persisted, instead of being regenerated (and
// thrown away) on every page load.

const CITIES = [
  { city: "Ghaziabad", state: "Uttar Pradesh", lat: 28.6692, lng: 77.4538 },
  { city: "Delhi", state: "Delhi", lat: 28.7041, lng: 77.1025 },
  { city: "Mumbai", state: "Maharashtra", lat: 19.0760, lng: 72.8777 },
  { city: "Chennai", state: "Tamil Nadu", lat: 13.0827, lng: 80.2707 },
  { city: "Kolkata", state: "West Bengal", lat: 22.5726, lng: 88.3639 },
  { city: "Bengaluru", state: "Karnataka", lat: 12.9716, lng: 77.5946 },
  { city: "Hyderabad", state: "Telangana", lat: 17.3850, lng: 78.4867 },
  { city: "Ahmedabad", state: "Gujarat", lat: 23.0225, lng: 72.5714 },
  { city: "Pune", state: "Maharashtra", lat: 18.5204, lng: 73.8567 },
  { city: "Jaipur", state: "Rajasthan", lat: 26.9124, lng: 75.7873 },
  { city: "Lucknow", state: "Uttar Pradesh", lat: 26.8467, lng: 80.9462 },
  { city: "Guwahati", state: "Assam", lat: 26.1445, lng: 91.7362 },
  { city: "Bhopal", state: "Madhya Pradesh", lat: 23.2599, lng: 77.4126 },
  { city: "Patna", state: "Bihar", lat: 25.5941, lng: 85.1376 },
  { city: "Chandigarh", state: "Chandigarh", lat: 30.7333, lng: 76.7794 },
  { city: "Thiruvananthapuram", state: "Kerala", lat: 8.5241, lng: 76.9366 },
  { city: "Bhubaneswar", state: "Odisha", lat: 20.2961, lng: 85.8245 },
  { city: "Dehradun", state: "Uttarakhand", lat: 30.3165, lng: 78.0322 },
  { city: "Srinagar", state: "Jammu & Kashmir", lat: 34.0837, lng: 74.7973 },
  { city: "Ranchi", state: "Jharkhand", lat: 23.3441, lng: 85.3096 },
  { city: "Raipur", state: "Chhattisgarh", lat: 21.2514, lng: 81.6296 },
  { city: "Amritsar", state: "Punjab", lat: 31.6340, lng: 74.8723 },
  { city: "Varanasi", state: "Uttar Pradesh", lat: 25.3176, lng: 82.9739 },
];

const EVENTS = [
  { key: "rainfall", label: "Rainfall" },
  { key: "thunderstorm", label: "Thunderstorm" },
  { key: "flooding", label: "Flooding" },
  { key: "heatwave", label: "Heatwave" },
  { key: "fog", label: "Fog" },
  { key: "dust_storm", label: "Dust Storm" },
  { key: "strong_winds", label: "Strong Winds" },
];

const SOURCES = ["X / Twitter", "Citizen Report App", "Weather API", "Public Dataset"];
const SNIPPETS = {
  rainfall: [
    "Heavy rain since the last hour, water pooling near the main market.",
    "Continuous drizzle across the area, visibility dropping on the highway.",
    "Sudden downpour, streets flooding near the railway crossing.",
  ],
  thunderstorm: [
    "Loud thunder and lightning for the past 20 minutes, power flickering.",
    "Dark clouds rolled in fast, thunderstorm warning feels accurate.",
    "Lightning strikes visible from the terrace, strong gusts too.",
  ],
  flooding: [
    "Waterlogging outside the metro station, knee-deep in places.",
    "Low-lying colony flooded after last night's rain, residents evacuating.",
    "River level rising fast, embankment road partially submerged.",
  ],
  heatwave: [
    "Unbearable heat since morning, feels well above the forecast.",
    "Heat advisory in effect, footpaths near empty by noon.",
    "Third day of extreme heat, hospitals reporting heat exhaustion cases.",
  ],
  fog: [
    "Dense fog since 4am, visibility under 50 metres on the expressway.",
    "Morning trains delayed due to thick fog cover.",
    "Fog still hasn't lifted by mid-morning, flights getting diverted.",
  ],
  dust_storm: [
    "Dust storm rolling in from the west, sky turned orange.",
    "Strong dust winds knocked over stalls near the bus stand.",
    "Visibility near zero for ten minutes during the dust squall.",
  ],
  strong_winds: [
    "Gusty winds uprooted a tree near the park entrance.",
    "Power lines swaying badly, one transformer tripped locally.",
    "Roof sheets flew off a shed during this evening's gusts.",
  ],
};

function pick(arr) {
  return arr[Math.floor(Math.random() * arr.length)];
}
function rand(min, max) {
  return min + Math.random() * (max - min);
}
function statusFromTrust(t) {
  if (t < 42) return "flagged";
  if (t < 68) return "pending";
  return "verified";
}

function generateSeedReports(n) {
  const now = Date.now();
  const out = [];
  for (let i = 0; i < n; i++) {
    const c = pick(CITIES);
    const ev = pick(EVENTS);
    const src = pick(SOURCES);
    const daysAgo = Math.floor(Math.pow(Math.random(), 2) * 30);
    const ts = now - daysAgo * 86400000 - Math.random() * 86400000;
    const trust = Math.round(rand(28, 99));
    const status = statusFromTrust(trust);
    out.push({
      _idx: i, // resolved to a real id after insertion — see below
      city: c.city,
      state: c.state,
      lat: c.lat + rand(-0.35, 0.35),
      lng: c.lng + rand(-0.35, 0.35),
      event: ev.key,
      source: src,
      ts,
      trust,
      status,
      // Same rule the live paths use (worker.js, routes/reports.js): only
      // "pending" is undecided. Most seeded "pending" rows are already
      // hours/days old by the time they're inserted, so the auto-resolve
      // sweep (server/autoResolve.js) picks them up and re-stamps them
      // within its first pass or two after boot — a good thing to point
      // out live during a demo, not a bug.
      decidedBy: status === "pending" ? null : "AI (initial score)",
      corroborationCount: 0,
      hasPhoto: Math.random() > 0.4,
      hasVideo: Math.random() > 0.78,
      text: pick(SNIPPETS[ev.key]),
      duplicateOf: null,
      mediaHash: null,
      mediaPath: null,
    });
  }

  // Seed a handful of duplicate pairs among lower-trust reports, same as the
  // original demo. _idx lines up with the id db.bulkSeed() will assign
  // (0..n-1 in array order), so we can reference it directly.
  const lowTrust = out.filter((r) => r.trust < 55);
  const pairs = Math.min(14, Math.floor(lowTrust.length / 2));
  for (let p = 0; p < pairs; p++) {
    const a = pick(lowTrust);
    const b = pick(lowTrust);
    if (a._idx === b._idx || b.duplicateOf !== null) continue;
    b.duplicateOf = a._idx;
    b.status = "flagged";
    b.decidedBy = "AI (initial score)";
    b.trust = Math.min(b.trust, 38);
  }

  return out.map(({ _idx, ...rest }) => rest);
}

module.exports = { generateSeedReports, CITIES, EVENTS, SOURCES };

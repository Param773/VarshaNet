// Shared state -> representative-city/coordinate lookup. Both the IMD CAP
// feed (imdCapIngest.js) and the NDMA SACHET national feed (sachetIngest.js)
// need to figure out which state an alert's free text is about, and give it
// a map-friendly city/lat/lng — this used to be duplicated in both files.
//
// Checked longest-name-first so "Uttar Pradesh" doesn't get shadowed by a
// shorter partial match.
const STATE_LOCATIONS = [
  { name: "Andhra Pradesh", city: "Vijayawada", lat: 16.5062, lng: 80.648 },
  { name: "Arunachal Pradesh", city: "Itanagar", lat: 27.0844, lng: 93.6053 },
  { name: "Himachal Pradesh", city: "Shimla", lat: 31.1048, lng: 77.1734 },
  { name: "Madhya Pradesh", city: "Bhopal", lat: 23.2599, lng: 77.4126 },
  { name: "Uttar Pradesh", city: "Lucknow", lat: 26.8467, lng: 80.9462 },
  { name: "West Bengal", city: "Kolkata", lat: 22.5726, lng: 88.3639 },
  { name: "Tamil Nadu", city: "Chennai", lat: 13.0827, lng: 80.2707 },
  { name: "Jammu and Kashmir", city: "Srinagar", lat: 34.0837, lng: 74.7973 },
  { name: "Chhattisgarh", city: "Raipur", lat: 21.2514, lng: 81.6296 },
  { name: "Uttarakhand", city: "Dehradun", lat: 30.3165, lng: 78.0322 },
  { name: "Maharashtra", city: "Mumbai", lat: 19.076, lng: 72.8777 },
  { name: "Karnataka", city: "Bengaluru", lat: 12.9716, lng: 77.5946 },
  { name: "Rajasthan", city: "Jaipur", lat: 26.9124, lng: 75.7873 },
  { name: "Telangana", city: "Hyderabad", lat: 17.385, lng: 78.4867 },
  { name: "Jharkhand", city: "Ranchi", lat: 23.3441, lng: 85.3096 },
  { name: "Meghalaya", city: "Shillong", lat: 25.5788, lng: 91.8933 },
  { name: "Nagaland", city: "Kohima", lat: 25.6751, lng: 94.1086 },
  { name: "Mizoram", city: "Aizawl", lat: 23.7271, lng: 92.7176 },
  { name: "Manipur", city: "Imphal", lat: 24.817, lng: 93.9368 },
  { name: "Tripura", city: "Agartala", lat: 23.8315, lng: 91.2868 },
  { name: "Sikkim", city: "Gangtok", lat: 27.3389, lng: 88.6065 },
  { name: "Gujarat", city: "Ahmedabad", lat: 23.0225, lng: 72.5714 },
  { name: "Haryana", city: "Chandigarh", lat: 30.7333, lng: 76.7794 },
  { name: "Punjab", city: "Amritsar", lat: 31.634, lng: 74.8723 },
  { name: "Kerala", city: "Thiruvananthapuram", lat: 8.5241, lng: 76.9366 },
  { name: "Odisha", city: "Bhubaneswar", lat: 20.2961, lng: 85.8245 },
  { name: "Assam", city: "Guwahati", lat: 26.1445, lng: 91.7362 },
  { name: "Bihar", city: "Patna", lat: 25.5941, lng: 85.1376 },
  { name: "Goa", city: "Panaji", lat: 15.4909, lng: 73.8278 },
  { name: "Delhi", city: "Delhi", lat: 28.7041, lng: 77.1025 },
];

// Fallback for a SACHET/IMD alert whose state couldn't be resolved from its
// text/author. Was previously { state: "India" } — putting the country's
// name in a *state* field, which then flowed straight into every
// state-level aggregate (Top States chart, the State filter dropdown, CSV
// exports) as if "India" were itself one of the 28 states/UTs. Fixed to
// "Delhi" to match the New Delhi coordinates it already falls back to, and
// to match the same fallback convention server/socialShared.js already
// uses correctly for unresolved Mastodon posts (DEFAULT_LOCATION there is
// { name: "New Delhi", state: "Delhi", ... }).
const DEFAULT_LOCATION = { state: "Delhi", city: "New Delhi", lat: 28.6139, lng: 77.209 };

// The SACHET national feed's <author> field carries the issuing office
// (e.g. "controlroom@ndma.gov.in (IMD Jaipur)"), not the state name itself —
// this maps the regional-office/SDMA names actually seen in that feed to
// their state, as an extra signal on top of the plain text search below.
const AUTHOR_HINT_TO_STATE = {
  "imd jaipur": "Rajasthan",
  "imd lucknow": "Uttar Pradesh",
  "imd chennai": "Tamil Nadu",
  "imd guwahati": "Assam",
  "imd ahmedabad": "Gujarat",
  "imd patna": "Bihar",
  "imd agartala": "Tripura",
  "imd dehradun": "Uttarakhand",
  "imd ranchi": "Jharkhand",
  "imd visakhapatnam": "Andhra Pradesh",
  "imd bhopal": "Madhya Pradesh",
  "imd bhubaneswar": "Odisha",
  "imd mumbai": "Maharashtra",
  "imd bengaluru": "Karnataka",
  "imd kolkata": "West Bengal",
  "imd shimla": "Himachal Pradesh",
  "imd srinagar": "Jammu and Kashmir",
  "imd chandigarh": "Haryana",
  "imd amritsar": "Punjab",
  "andhra pradesh sdma": "Andhra Pradesh",
  "asdma": "Assam",
  "ksdma": "Kerala",
  "tnsdma": "Tamil Nadu",
};

function detectState(text) {
  const lower = (text || "").toLowerCase();
  const match = STATE_LOCATIONS.find((s) => lower.indexOf(s.name.toLowerCase()) > -1);
  return match || null;
}

function detectStateFromAuthor(author) {
  const lower = (author || "").toLowerCase();
  for (const hint in AUTHOR_HINT_TO_STATE) {
    if (lower.indexOf(hint) > -1) {
      const stateName = AUTHOR_HINT_TO_STATE[hint];
      return STATE_LOCATIONS.find((s) => s.name === stateName) || null;
    }
  }
  return null;
}

module.exports = { STATE_LOCATIONS, DEFAULT_LOCATION, detectState, detectStateFromAuthor };

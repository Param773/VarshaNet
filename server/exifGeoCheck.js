// EXIF GPS cross-verification for citizen-submitted photos.
//
// server/imageAuthenticity.js asks Claude's vision whether a photo LOOKS
// like real outdoor evidence of the claimed weather event — but it never
// checks WHERE the photo was actually taken against the location the
// citizen reported. A camera/phone that has location tagging enabled
// embeds the capture coordinates in the JPEG's EXIF GPS IFD; if that's
// present, it's an independent, harder-to-fake signal than the browser
// geolocation captured at submission (server/routes/reports.js's
// lat/lng), because it's baked into the file by the device at the moment
// the photo was taken rather than read from the browser at upload time.
//
// This is deliberately soft, for two reasons:
//   1. Most submitted photos won't have it at all. iPhones strip GPS EXIF
//      from anything shared via a share sheet unless "Location" is left
//      on for that share; WhatsApp/Telegram/Instagram strip EXIF entirely
//      on re-compression; screenshots and downloaded/forwarded images
//      never had it. Absence is the common case, not a red flag — a
//      report must never be penalized just because this data isn't there.
//   2. Even when present, it's the coordinates of the CAMERA at capture
//      time, not a guarantee the photo shows what's happening in that
//      city right now — someone could genuinely be reporting weather
//      they're experiencing while traveling, or the phone's clock/GPS
//      fix could be stale/inaccurate. So this nudges the trust score,
//      the same "moderate factor, silent when unavailable" treatment
//      server/scoring.js already gives the AI photo review.
//
// Only JPEGs carry EXIF in practice (PNG/WebP essentially never do in the
// wild) — this quietly returns null for anything else rather than trying
// and failing.

const ExifParser = require("exif-parser");

const EARTH_RADIUS_KM = 6371;

function toRadians(deg) {
  return (deg * Math.PI) / 180;
}

// Great-circle distance between two lat/lng points, in km.
function haversineKm(lat1, lng1, lat2, lng2) {
  const dLat = toRadians(lat2 - lat1);
  const dLng = toRadians(lng2 - lng1);
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toRadians(lat1)) * Math.cos(toRadians(lat2)) * Math.sin(dLng / 2) ** 2;
  return EARTH_RADIUS_KM * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

// Weather events are local phenomena, so the bar for "matches" is
// generous-metro-area, not GPS-exact — a report filed for a city can
// legitimately be photographed from its outskirts. "Far" is picked to
// mean "a different city/region entirely", not "wrong side of town".
const MATCH_MAX_KM = 50;
const FAR_MIN_KM = 150;

/**
 * Extracts GPS coordinates embedded in a photo's EXIF data, if any.
 * @param {Buffer} imageBuffer
 * @param {string} mimeType
 * @returns {{lat: number, lng: number} | null}
 */
function extractExifGps(imageBuffer, mimeType) {
  if (!imageBuffer || !mimeType || mimeType.toLowerCase().trim() !== "image/jpeg") {
    return null;
  }
  try {
    const result = ExifParser.create(imageBuffer).parse();
    const { GPSLatitude, GPSLongitude } = result.tags || {};
    if (typeof GPSLatitude !== "number" || typeof GPSLongitude !== "number") return null;
    if (Number.isNaN(GPSLatitude) || Number.isNaN(GPSLongitude)) return null;
    // (0, 0) is "null island" — the value some devices/libraries write
    // when a GPS fix was attempted but never actually acquired. Treat it
    // as "no usable GPS data" rather than a real coordinate.
    if (GPSLatitude === 0 && GPSLongitude === 0) return null;
    return { lat: GPSLatitude, lng: GPSLongitude };
  } catch (e) {
    // Not a parseable JPEG / no EXIF segment / corrupt data — all just
    // mean "nothing to check", never an error worth surfacing upward.
    return null;
  }
}

/**
 * Cross-verifies a photo's embedded EXIF GPS (if any) against the
 * location reported with the submission.
 *
 * @param {Object} o
 * @param {Buffer} o.imageBuffer
 * @param {string} o.mimeType
 * @param {number|null|undefined} o.reportedLat
 * @param {number|null|undefined} o.reportedLng
 * @returns {{exifLat: number, exifLng: number, distanceKm: number, match: "close"|"far"|"ambiguous"} | null}
 *   null when there's no EXIF GPS to check, or no reported location to
 *   check it against — in both cases this factor simply has no opinion.
 */
function verifyPhotoLocation({ imageBuffer, mimeType, reportedLat, reportedLng }) {
  const gps = extractExifGps(imageBuffer, mimeType);
  if (!gps) return null;

  if (
    typeof reportedLat !== "number" ||
    typeof reportedLng !== "number" ||
    Number.isNaN(reportedLat) ||
    Number.isNaN(reportedLng)
  ) {
    return null;
  }

  const distanceKm = haversineKm(gps.lat, gps.lng, reportedLat, reportedLng);
  let match;
  if (distanceKm <= MATCH_MAX_KM) match = "close";
  else if (distanceKm >= FAR_MIN_KM) match = "far";
  else match = "ambiguous"; // between the two thresholds — not confident enough either way

  return {
    exifLat: gps.lat,
    exifLng: gps.lng,
    distanceKm: Math.round(distanceKm),
    match,
  };
}

module.exports = { verifyPhotoLocation, extractExifGps, haversineKm };

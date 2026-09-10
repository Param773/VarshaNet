// Trust-scoring logic, ported 1:1 from the original front-end prototype so
// the scoring behaviour judges saw in the demo doesn't change — it just now
// runs server-side, against data nobody can tamper with from devtools.

const { classifyReportText } = require("./mlClassifier");

// Weather-category keywords used both to auto-categorize a report's text
// and to score how well it matches its claimed category. Covers English
// plus nine major Indian languages (Hindi, Bengali, Marathi, Tamil,
// Telugu, Kannada, Malayalam, Gujarati, Punjabi, Urdu) — together the
// first languages of roughly three in four people in India, so a post
// written entirely in one of them (not just English, and not just the
// couple of hardcoded Hindi phrases this used to have) can still be
// detected and categorized correctly.
//
// This is still keyword/substring matching, same as the English list
// always was — not real NLP. It has no language-identification step, no
// spelling-variant or dialect tolerance, no stemming, and no handling of
// regional words typed in Latin script (e.g. "baarish", "paani bhar
// gaya"). A post mixing English with a regional language — very common
// on Indian social media — still matches on whichever language it
// contains, which covers the common case even without solving the
// general problem.
const CATEGORY_KEYWORDS = {
  rainfall: [
    "rain", "rainfall", "drizzle", "downpour", "showers", "pouring", "monsoon",
    "बारिश", "वर्षा", "बरसात", // Hindi
    "বৃষ্টি", // Bengali
    "पाऊस", // Marathi
    "மழை", // Tamil
    "వర్షం", // Telugu
    "ಮಳೆ", // Kannada
    "മഴ", // Malayalam
    "વરસાદ", // Gujarati
    "ਮੀਂਹ", // Punjabi
    "بارش", // Urdu
  ],
  thunderstorm: [
    "thunder", "lightning", "storm", "gust", "squall",
    "बिजली", "गरज", // Hindi
    "বজ্রঝড়", "বাজ", // Bengali
    "वीज", "गडगडाट", // Marathi
    "இடி", "மின்னல்", // Tamil
    "పిడుగు", "మెరుపు", // Telugu
    "ಸಿಡಿಲು", "ಮಿಂಚು", // Kannada
    "ഇടിമിന്നൽ", // Malayalam
    "ગાજવીજ", // Gujarati
    "ਬਿਜਲੀ", "ਗਰਜ", // Punjabi
    "بجلی کی چمک", // Urdu
  ],
  flooding: [
    "flood", "waterlog", "submerged", "overflow", "inundat", "knee-deep",
    "बाढ़", // Hindi
    "বন্যা", // Bengali
    "पूर", // Marathi
    "வெள்ளம்", // Tamil
    "వరద", // Telugu
    "ಪ್ರವಾಹ", // Kannada
    "വെള്ളപ്പൊക്കം", // Malayalam
    "પૂર", // Gujarati
    "ਹੜ੍ਹ", // Punjabi
    "سیلاب", // Urdu
  ],
  heatwave: [
    "heat", "scorching", "hot", "heatwave", "sweltering",
    "गर्मी", "लू", // Hindi
    "গরম", "তাপপ্রবাহ", // Bengali
    "उष्णता", "उष्माघात", // Marathi
    "வெப்பம்", // Tamil
    "వేడి", "వడగాడ్పు", // Telugu
    "ಶಾಖ", "ಬಿಸಿಗಾಳಿ", // Kannada
    "ചൂട്", "ഉഷ്ണതരംഗം", // Malayalam
    "ગરમી", "લૂ", // Gujarati
    "ਗਰਮੀ", "ਲੂ", // Punjabi
    "گرمی", "لو", // Urdu
  ],
  fog: [
    "fog", "mist", "visibility", "haze",
    "कोहरा", // Hindi
    "কুয়াশা", // Bengali
    "धुके", // Marathi
    "பனிமூட்டம்", // Tamil
    "పొగమంచు", // Telugu
    "ಮಂಜು", // Kannada
    "മൂടൽമഞ്ഞ്", // Malayalam
    "ધુમ્મસ", // Gujarati
    "ਧੁੰਦ", // Punjabi
    "دھند", // Urdu
  ],
  dust_storm: [
    "dust", "sandstorm", "dust storm", "orange sky",
    "आंधी", "धूल", // Hindi
    "ধুলিঝড়", // Bengali
    "धुळीचे वादळ", // Marathi
    "தூசி புயல்", // Tamil
    "దుమ్ము తుఫాను", // Telugu
    "ಧೂಳಿನ ಬಿರುಗಾಳಿ", // Kannada
    "പൊടിക്കാറ്റ്", // Malayalam
    "ધૂળની ડમરી", // Gujarati
    "ਧੂੜ ਦਾ ਤੂਫਾਨ", // Punjabi
    "دھول کا طوفان", // Urdu
  ],
  strong_winds: [
    "wind", "gust", "gale", "uprooted", "blown",
    "तेज़ हवा", // Hindi
    "ঝোড়ো হাওয়া", // Bengali
    "जोरदार वारा", // Marathi
    "பலத்த காற்று", // Tamil
    "బలమైన గాలులు", // Telugu
    "ಬಿರುಗಾಳಿ", // Kannada
    "ശക്തമായ കാറ്റ്", // Malayalam
    "તેજ પવન", // Gujarati
    "ਤੇਜ਼ ਹਵਾ", // Punjabi
    "تیز ہوا", // Urdu
  ],
};

const WEATHER_CONFLICTS = {
  rainfall: ["Clear"],
  flooding: ["Clear"],
  thunderstorm: ["Clear"],
  heatwave: ["Rain", "Snow", "Drizzle"],
  dust_storm: ["Rain"],
  fog: ["Clear"],
  strong_winds: [],
};

const SUSPICIOUS_WORDS = ["fake", "joke", "prank", "not real", "just kidding", "clickbait"];

function statusFromTrust(t) {
  if (t < 42) return "flagged";
  if (t < 68) return "pending";
  return "verified";
}

/**
 * @param {Object} o
 * @param {string} o.description
 * @param {string} o.event         - category key, e.g. "rainfall"
 * @param {boolean} o.hasMedia
  * @param {boolean} o.mediaReused  - true if this file's hash matches an existing report
 * @param {boolean} [o.mediaNearDuplicate] - true if this image closely resembles (but isn't byte-identical to) an existing report's media
 * @param {boolean} [o.textReused] - true if this report's text+city+event matches a recent existing report (see textDedup.js)
 * @param {string|null} o.officialMain - live weather "main" condition for the city, or null
 * @param {string} o.city
 */
function scoreReport(o) {
  let score = 50;
  const reasons = [];
  const desc = (o.description || "").trim();
  const lower = desc.toLowerCase();
  SUSPICIOUS_WORDS.forEach((w) => {
    if (lower.indexOf(w) > -1) {
      score -= 40;
      reasons.push(`Contains suspicious phrase: "${w}"`);
    }
  });

  const mlResult = classifyReportText(desc);
  if (mlResult && mlResult.confidence > 0.65) {
    const pct = Math.round(mlResult.confidence * 100);
    if (mlResult.label === "suspicious") {
      score -= 20;
      reasons.push(`ML text classifier flagged this description as potentially misleading (${pct}% confidence)`);
    } else {
      score += 5;
      reasons.push(`ML text classifier assessed this description as consistent with genuine reports (${pct}% confidence)`);
    }
  }

  const keywords = CATEGORY_KEYWORDS[o.event] || [];
  const matched = keywords.some((k) => lower.indexOf(k) > -1);
  if (matched) {
    score += 15;
    reasons.push("Description text is consistent with reported category");
  } else if (desc.length > 0) {
    score -= 10;
    reasons.push("Description does not clearly match the reported category");
  }
  if (desc.length < 10) {
    score -= 5;
    reasons.push("Description is very short / low detail");
  }

  const exclaims = (desc.match(/!/g) || []).length;
  const capsRatio = (desc.match(/[A-Z]/g) || []).length / Math.max(1, desc.length);
  if (exclaims > 4 || capsRatio > 0.5) {
    score -= 10;
    reasons.push("Text style resembles spam (excessive caps/punctuation)");
  }

  score += 5;
  reasons.push("Direct citizen report (GPS/location captured at submission)");

  if (o.hasMedia) {
    score += 10;
    reasons.push("Report includes photo/video evidence");
  } else {
    score -= 5;
    reasons.push("No photo/video evidence attached");
  }

    if (o.mediaReused) {
    score -= 30;
    reasons.push("Identical media file (by content hash) already used in another report");
  } else if (o.mediaNearDuplicate) {
    score -= 15;
    reasons.push("Media closely resembles another submitted photo (possible re-upload or edited copy)");
  }

  if (o.textReused) {
    score -= 30;
    reasons.push("Same wording for this city/event was already reported recently (likely a re-published alert)");
  }

  if (o.officialMain) {
    const conflicts = WEATHER_CONFLICTS[o.event] || [];
    if (conflicts.indexOf(o.officialMain) > -1) {
      score -= 25;
      reasons.push(
        `Live weather data shows "${o.officialMain}" for ${o.city}, which conflicts with the reported category`
      );
    } else {
      score += 10;
      reasons.push(`Report is broadly consistent with live weather data for ${o.city}`);
    }
  }

  score = Math.max(0, Math.min(100, Math.round(score)));
  return { trustScore: score, reasons };
}
function detectCategory(text) {
  const lower = (text || "").toLowerCase();
  if (!lower.trim()) return null;

  let bestKey = null;
  let bestCount = 0;
  Object.keys(CATEGORY_KEYWORDS).forEach((key) => {
    const count = CATEGORY_KEYWORDS[key].reduce(
      (acc, kw) => acc + (lower.indexOf(kw) > -1 ? 1 : 0),
      0
    );
    if (count > bestCount) {
      bestCount = count;
      bestKey = key;
    }
  });
  return bestKey;
}

module.exports = {
  scoreReport,
  statusFromTrust,
  detectCategory,
  CATEGORY_KEYWORDS,
  WEATHER_CONFLICTS,
  SUSPICIOUS_WORDS,
};
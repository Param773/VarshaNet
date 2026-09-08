const jwt = require("jsonwebtoken");

function requireAdmin(req, res, next) {
  const header = req.headers.authorization || "";
  const token = header.startsWith("Bearer ") ? header.slice(7) : null;
  if (!token) return res.status(401).json({ error: "Missing token" });
  try {
    req.admin = jwt.verify(token, process.env.JWT_SECRET);
    // Older sessions signed before roles existed won't have a role claim —
    // treat them as full "admin" so an already-logged-in user isn't
    // suddenly locked out mid-session; they'll get a role claim on their
    // next login anyway.
    if (!req.admin.role) req.admin.role = "admin";
    next();
  } catch (e) {
    return res.status(401).json({ error: "Invalid or expired session" });
  }
}

// Role-gate for routes that only some admin tiers should reach — e.g.
// requireRole("admin", "moderator") lets those two through and blocks
// "analyst". Always used after requireAdmin, which populates req.admin.
function requireRole(...allowedRoles) {
  return function (req, res, next) {
    if (!req.admin || !allowedRoles.includes(req.admin.role)) {
      return res.status(403).json({ error: "Your role does not have permission to do this." });
    }
    next();
  };
}

module.exports = { requireAdmin, requireRole };

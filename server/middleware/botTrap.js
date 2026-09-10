const MIN_FILL_TIME_MS = 3000;

function botTrap(req, res, next) {
  const honeypot = req.body?.website;
  const formLoadedAt = parseInt(req.body?.formLoadedAt, 10);

  if (honeypot) {
    return res.status(400).json({ error: "Submission rejected." });
  }

  if (!Number.isNaN(formLoadedAt)) {
    const elapsed = Date.now() - formLoadedAt;
    if (elapsed < MIN_FILL_TIME_MS) {
      return res.status(400).json({ error: "Submission rejected." });
    }
  }

  next();
}

module.exports = { botTrap };

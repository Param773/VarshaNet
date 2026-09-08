# VarshaNet — Backend + Deployable Site

Smart India Hackathon 2026 (PS 26069) — National Weather Big Data Analytics Platform,
for the Ministry of Earth Sciences / IMD.

This wraps your original `varshanet.html` frontend with a real Node.js/Express backend, so
it's a working full-stack app instead of a single static file that resets every page load.

## What changed from the original HTML file

The original file looked and worked great, but everything lived only in the browser:
- 260 "reports" were randomly generated fresh on every page load and vanished on refresh
- The trust-scoring algorithm ran in the browser, so anyone could open devtools and fake a score
- The admin login accepted **any** username/password
- "Duplicate media" detection just compared file name + size, not actual file content

This version keeps the exact same look, pages, and scoring logic, but moves the parts that
matter server-side:

| Feature | Before | Now |
|---|---|---|
| Reports storage | In-memory, gone on refresh | Persisted in `data/reports.json` on the server |
| Trust scoring | Computed in the browser | Computed in `server/scoring.js` on the server |
| Weather cross-check | Browser called Open-Meteo directly | Server calls Open-Meteo (`server/weather.js`) |
| Admin login | Any username/password worked | Real login: bcrypt password + JWT session |
| Approve / Reject | Client-side only, not real moderation | Requires a valid admin JWT (`PATCH /api/reports/:id/status`) |
| Duplicate media check | Filename + file size | Actual SHA-256 hash of the file content |
| Historical demo data | Regenerated randomly every load | Generated once, seeded into the database on first boot |

Nothing about the UI, styling, or page structure was touched — only the `<script>` logic that
talked to fake in-memory data now talks to real API endpoints.

## Live data sources

On top of citizen-submitted reports, the server auto-ingests from five live sources on a timer (and on-demand via the admin "Pull Live Data" button):

| Source | File | Feed | Interval |
|---|---|---|---|
| Weather API | `server/ingest.js` | Open-Meteo, 200 Indian cities | 20 min |
| Public Dataset | `server/sachetIngest.js` | NDMA SACHET government CAP alerts | 30 min |
| IMD API | `server/imdCapIngest.js` | IMD's own official CAP alert feed | 30 min |
| Social Media (Reddit) | `server/socialIngest.js` | Reddit's free public search (keyword/hashtag based) | 15 min |
| Social Media (Mastodon) | `server/mastodonIngest.js` | Mastodon's free public hashtag-timeline API | 15 min |

**Why Reddit and Mastodon, and not Twitter/X:** Twitter/X API v2's search endpoint (needed to look up `#IMD`-style hashtags) has required a paid Basic-tier developer plan since 2023 — there's no free, keyless way to search live tweets. Bluesky looked like an easy substitute, but its `app.bsky.feed.searchPosts` endpoint actually requires an authenticated app-password session too (checked live while building this — it's not paid, but it isn't keyless either). Reddit's public search JSON endpoint and Mastodon's public hashtag-timeline endpoint (`GET /api/v1/timelines/tag/:hashtag`) are both genuinely free and keyless, so those are the two real live social-media sources for the demo — which also makes "social media platforms" (plural) in the problem statement actually true, not just one source relabelled. `socialIngest.js`/`mastodonIngest.js` share city/hashtag-guessing logic via `server/socialShared.js` but run as independent pipelines, and either (or a future Twitter/X adapter, once a paid key is available) can be swapped without touching scoring or database code — only the fetch call at the top of each pipeline changes.

Worth being upfront about: Mastodon's Indian-weather-topic userbase is far smaller than Reddit's (or Twitter's, historically), so this adapter typically produces fewer reports per run — a real reach limitation of the platform, not a bug in the adapter.

## Project structure

```
varshanet/
├── public/
│   └── index.html        # your original frontend, wired up to the API
├── server/
│   ├── index.js           # Express app entry point
│   ├── db.js               # simple JSON-file data store (data/reports.json)
│   ├── scoring.js          # trust-scoring algorithm (ported from the client)
│   ├── weather.js          # Open-Meteo geocoding + forecast proxy
│   ├── seedData.js         # generates the initial demo history
│   ├── socialShared.js     # city/hashtag helpers shared by both social adapters
│   ├── socialIngest.js     # live social-media ingestion (Reddit)
│   ├── mastodonIngest.js   # live social-media ingestion (Mastodon)
│   ├── middleware/auth.js  # JWT check for admin-only routes
│   └── routes/
│       ├── reports.js      # GET/POST reports, PATCH approve/reject
│       ├── admin.js        # POST /api/admin/login
│       └── weather.js      # GET /api/weather?city=
├── scripts/
│   ├── hash-password.js    # generates a bcrypt hash for your admin password
│   └── seed.js              # manual reseed helper (server auto-seeds on first boot anyway)
├── data/                    # created automatically — reports.json + uploads/ (gitignored)
├── .env.example
├── package.json
└── README.md
```

## Run it locally

You need Node.js 18 or newer (for the built-in `fetch`).

```bash
cd varshanet
npm install
cp .env.example .env
```

Now generate a real bcrypt hash for whatever admin password you want to use:

```bash
npm run hash-password -- "yourStrongPassword"
```

Copy the printed hash into `.env` as `ADMIN_PASSWORD_HASH`. Also set `ADMIN_USERNAME`
and a random `JWT_SECRET` (the `.env.example` file has a one-liner to generate one).

Then start the server:

```bash
npm start
```

Open **http://localhost:3000** — that's the whole app, frontend and backend on one port.
On first boot it auto-seeds ~220 historical demo reports into `data/reports.json` so the
dashboard isn't empty; every report you submit through the form after that is real and
persists across restarts.

Sign in to the Admin Console with the username/password from your `.env`.

## Environment variables (`.env`)

| Variable | What it's for |
|---|---|
| `PORT` | Port the server listens on (defaults to 3000) |
| `JWT_SECRET` | Random secret used to sign admin login sessions — keep this private |
| `ADMIN_USERNAME` | Admin Console username |
| `ADMIN_PASSWORD_HASH` | Bcrypt hash of the admin password (never store the plain password) |

**Never commit your real `.env` file.** `.gitignore` already excludes it.

## Deploying it

This is a plain Node/Express app with no native dependencies, so it runs on pretty much
any Node host. Two popular free options for a hackathon demo:

### Render.com
1. Push this folder to a GitHub repo.
2. On Render: **New → Web Service**, connect the repo.
3. Build command: `npm install`. Start command: `npm start`.
4. Add the four environment variables from `.env` under **Environment**.
5. Deploy. Render gives you a public HTTPS URL.

### Railway.app
1. Push to GitHub, then **New Project → Deploy from GitHub repo** on Railway.
2. It auto-detects Node and runs `npm install && npm start`.
3. Add the same environment variables under the **Variables** tab.
4. Deploy — Railway gives you a public URL too.

Either works fine for SIH judging. A VPS (DigitalOcean, etc.) with `pm2 start server/index.js`
works the same way if you'd rather run it yourself.

### One important caveat: the filesystem is not permanent on most free hosting

`data/reports.json` and uploaded media files live on disk. Render's and Railway's **free**
tiers use an ephemeral filesystem — anything written to disk gets wiped on redeploy or
restart. For a hackathon demo/judging session this is completely fine (the app reseeds
demo data automatically on boot, and reports submitted during a live demo stay put until
the next deploy). If you want reports to survive redeploys long-term, either:
- add a paid persistent disk (Render "Disks" add-on, Railway volumes), or
- swap `server/db.js` for a real database (Postgres/MongoDB) — every other file only
  calls the functions exported from `db.js`, so that's the only file you'd need to change.

## API reference

| Method | Route | Auth | Purpose |
|---|---|---|---|
| `GET` | `/api/reports` | none | List all reports |
| `POST` | `/api/reports` | none | Submit a citizen report (`multipart/form-data`: `category`, `description`, `city`, `state`, `lat`?, `lng`?, `media`?) |
| `PATCH` | `/api/reports/:id/status` | admin JWT | Approve (`"verified"`) or reject (`"rejected"`) a report |
| `POST` | `/api/admin/login` | none | `{ username, password }` → `{ token }` |
| `POST` | `/api/admin/ingest` | admin JWT | Manually trigger all five live ingestion jobs now, returns a per-source breakdown |
| `GET` | `/api/weather?city=` | none | Live weather lookup via Open-Meteo (used by the Forecast page) |

Admin routes expect `Authorization: Bearer <token>` from the login response.

## Adding another admin account

Right now there's a single admin identity read from `.env`. If you need more than one
admin login, the simplest change is swapping the single-account check in
`server/routes/admin.js` for a small `data/admins.json` list of `{ username, passwordHash }`
— ask if you want a hand wiring that up.

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

## Architecture: streaming ingestion (Kafka)

The honest answer to "which big data tools are you using": **Apache Kafka** (any
Kafka-API-compatible broker — Redpanda locally, see below) as a message broker between
ingestion and processing, with a horizontally-scalable **consumer-group worker pool** doing
the actual scoring/classification/persistence. This isn't a bolted-on demo feature — it's
the real path every auto-ingested report takes:

```
 ┌─────────────────┐  ┌──────────────┐  ┌────────────────┐  ┌──────────────┐  ┌───────────────────┐
 │ server/ingest.js │  │ sachetIngest │  │ imdCapIngest.js │  │ socialIngest │  │ mastodonIngest.js │   ← 5 producers,
 │  (Weather API)   │  │  (SACHET)    │  │   (IMD CAP)     │  │  (Reddit)    │  │    (Mastodon)     │     independent timers
 └────────┬─────────┘  └──────┬───────┘  └────────┬────────┘  └──────┬───────┘  └─────────┬─────────┘
          └─────────────────────────────┬──────────────────────────────────────────────────┘
                                          ▼
                          Kafka topic: varshanet.raw-reports
                          (server/reportProducer.js publishes, partitioned by city)
                                          ▼
              ┌──────────────────────────┴──────────────────────────┐
              ▼                           ▼                          ▼
     server/worker.js #1         server/worker.js #2         server/worker.js #N   ← same consumer
     (scoreReport + db.addReport)  (scoreReport + db.addReport)  ...                  group: Kafka splits
              └──────────────────────────┬──────────────────────────┘                 partitions across them
                                          ▼
                                     MongoDB
```

- **Producers** (`server/ingest.js`, `sachetIngest.js`, `imdCapIngest.js`, `socialIngest.js`,
  `mastodonIngest.js`): each pipeline's job now ends the moment it finds a candidate report —
  it publishes the raw fields onto `varshanet.raw-reports` (`server/reportProducer.js`) and
  moves straight on to the next item, instead of blocking on a scoring computation and a
  database round-trip per item.
- **Consumer** (`server/worker.js`): the only place `scoreReport()`/`db.addReport()` run for
  auto-ingested data now — one implementation instead of five duplicated ones. It's a
  `kafkajs` consumer-group member (`KAFKA_CONSUMER_GROUP`, default
  `varshanet-scoring-workers`); run more than one instance of this same file and Kafka
  automatically splits the topic's partitions across them — genuine distributed processing,
  scaled by starting another process, not by writing more code.
- **Reliability**: a message that fails to score/persist is routed to
  `varshanet.raw-reports.dlq` (a dead-letter topic) instead of being silently dropped, so it
  can be inspected or replayed later.
- **Citizen submissions** (`POST /api/reports`) stay synchronous — scored and saved
  immediately, same request/response the frontend always expected — but also publish a
  fire-and-forget event onto `varshanet.activity` afterward, so every report in the system,
  auto-ingested or citizen-submitted, touches the same Kafka stream.

**Local dev broker:** `docker-compose.yml` runs a single-node [Redpanda](https://redpanda.com)
container — Kafka-API-compatible, no separate Zookeeper process needed, one command
(`docker compose up -d`) instead of standing up a real Kafka cluster. Redpanda Console
(a Kafka web UI) comes with it at `http://localhost:8080` — genuinely useful to have open
during a live demo, since you can watch messages land on `varshanet.raw-reports` and get
consumed in real time.

**Production/deployment broker:** point `KAFKA_BROKERS` (+ `KAFKA_USERNAME`/`KAFKA_PASSWORD`/
`KAFKA_SSL=true`) at a managed Kafka-API-compatible cloud broker instead — e.g.
[Redpanda Serverless](https://redpanda.com) (Kafka-API compatible, reachable over the public
internet via TLS, so it works from Render/Railway without you having to self-host a broker
container). No code changes either way — `server/kafka.js` is the only file that reads those
env vars.

If a judge specifically asks about Spark or Flink: this project doesn't use them, on purpose.
Those are batch/large-scale distributed *compute* engines for processing already-landed data
at a scale (many machines, huge datasets) this project doesn't operate at. What Kafka +
a consumer-group worker pool gives instead — decoupled, ordered-per-key, horizontally-scalable
*stream* processing between five independent live data sources and a database — is the
architecturally correct tool for this specific problem (continuous ingestion from multiple
live feeds), not a name-drop for its own sake.

## Project structure

```
varshanet/
├── public/
│   └── index.html          # your original frontend, wired up to the API
├── server/
│   ├── index.js             # Express app entry point
│   ├── worker.js            # Kafka consumer — scores + persists queued reports (npm run worker)
│   ├── kafka.js              # shared Kafka client (producer + consumer factory)
│   ├── topics.js             # Kafka topic name constants
│   ├── reportProducer.js     # publishRawReport() — shared by all 5 ingestion pipelines
│   ├── db.js                  # MongoDB-backed data store
│   ├── scoring.js             # trust-scoring algorithm (ported from the client)
│   ├── weather.js             # Open-Meteo geocoding + forecast proxy
│   ├── seedData.js            # generates the initial demo history
│   ├── socialShared.js        # city/hashtag helpers shared by both social adapters
│   ├── socialIngest.js        # live social-media ingestion (Reddit) — Kafka producer
│   ├── mastodonIngest.js      # live social-media ingestion (Mastodon) — Kafka producer
│   ├── sachetIngest.js        # NDMA SACHET CAP alerts — Kafka producer
│   ├── imdCapIngest.js        # IMD's own CAP alert feed — Kafka producer
│   ├── ingest.js              # Open-Meteo weather scan, 200 cities — Kafka producer
│   ├── middleware/auth.js     # JWT check for admin-only routes
│   └── routes/
│       ├── reports.js         # GET/POST reports, PATCH approve/reject
│       ├── admin.js           # POST /api/admin/login, manual ingest trigger
│       └── weather.js         # GET /api/weather?city=
├── scripts/
│   ├── hash-password.js    # generates a bcrypt hash for your admin password
│   └── seed.js              # manual reseed helper (server auto-seeds on first boot anyway)
├── docker-compose.yml       # local dev Kafka broker (Redpanda) + web UI
├── .env.example
├── package.json
└── README.md
```

## Run it locally

You need Node.js 18 or newer (for the built-in `fetch`) and Docker (for the local Kafka broker).

```bash
cd varshanet
npm install
cp .env.example .env
docker compose up -d      # starts the local Redpanda broker + console UI
```

Now generate a real bcrypt hash for whatever admin password you want to use:

```bash
npm run hash-password -- "yourStrongPassword"
```

Copy the printed hash into `.env` as `ADMIN_PASSWORD_HASH`. Also set `ADMIN_USERNAME`,
a random `JWT_SECRET`, and your `MONGODB_URI` (a free MongoDB Atlas cluster is enough —
see `.env.example` for all of it, Kafka vars included).

Then start the worker and the web app — **two separate terminals**, since that's the whole
point (they scale independently):

```bash
npm run worker    # terminal 1 — Kafka consumer: scores + saves queued reports
npm start          # terminal 2 — Express app + the 5 ingestion producers
```

(Or set `RUN_WORKER_INPROCESS=true` in `.env` and just run `npm start` — starts the consumer
inside the same process, handy for quick local testing. Keep this unset for any real
deployment; see `server/worker.js`'s header comment for why.)

Open **http://localhost:3000** — that's the whole app, frontend and backend on one port.
On first boot it auto-seeds ~220 historical demo reports into MongoDB so the dashboard isn't
empty; every report you submit through the form after that is real and persists across
restarts. Open **http://localhost:8080** for Redpanda Console to watch messages flow through
`varshanet.raw-reports` in real time.

Sign in to the Admin Console with the username/password from your `.env`.

## Environment variables (`.env`)

| Variable | What it's for |
|---|---|
| `PORT` | Port the server listens on (defaults to 3000) |
| `JWT_SECRET` | Random secret used to sign admin login sessions — keep this private |
| `ADMIN_USERNAME` | Admin Console username |
| `ADMIN_PASSWORD_HASH` | Bcrypt hash of the admin password (never store the plain password) |
| `MONGODB_URI` | MongoDB connection string (see `server/db.js`) |
| `KAFKA_BROKERS` | Comma-separated broker host:port list (`localhost:19092` for the local Redpanda container) |
| `KAFKA_USERNAME` / `KAFKA_PASSWORD` / `KAFKA_SASL_MECHANISM` | SASL credentials for a managed cloud broker — leave unset for local dev |
| `KAFKA_SSL` | `true` for a managed cloud broker, `false`/unset for local dev |
| `KAFKA_CONSUMER_GROUP` | Consumer group `server/worker.js` joins — same value across multiple worker instances to scale horizontally |
| `WORKER_PORT` | Port `server/worker.js`'s standalone health endpoint listens on |
| `RUN_WORKER_INPROCESS` | Local-dev convenience — runs the consumer inside `npm start` instead of a separate `npm run worker` |

**Never commit your real `.env` file.** `.gitignore` already excludes it.

## Deploying it

This needs three things running: the web app, at least one worker, and a Kafka-API-compatible
broker.

### The broker
Don't self-host a broker container for a public deployment unless you're already comfortable
with Docker networking on your host — point `KAFKA_BROKERS`/`KAFKA_USERNAME`/
`KAFKA_PASSWORD`/`KAFKA_SSL=true` at a managed one instead (e.g. Redpanda Serverless), reachable
over the public internet via TLS from wherever the app runs.

### The web app (`npm start`)
### Render.com
1. Push this folder to a GitHub repo.
2. On Render: **New → Web Service**, connect the repo.
3. Build command: `npm install`. Start command: `npm start`.
4. Add the environment variables from `.env` under **Environment**.
5. Deploy. Render gives you a public HTTPS URL.

### Railway.app
1. Push to GitHub, then **New Project → Deploy from GitHub repo** on Railway.
2. It auto-detects Node and runs `npm install && npm start`.
3. Add the same environment variables under the **Variables** tab.
4. Deploy — Railway gives you a public URL too.

### The worker (`npm run worker`)
This needs to run continuously, separately from the web app — worth knowing before you pick a
host:
- **Railway**: add a second service in the same project (**New → Empty Service**, same repo,
  start command `npm run worker`), same environment variables. Straightforward — no separate
  billing category for "background" work the way Render has.
- **Render**: Render's free tier only covers the **Web Service** type; a **Background Worker**
  service (the correct type for `npm run worker`) needs a paid instance (from ~$7/mo). If you
  want to stay fully free on Render, deploy the worker as a second free **Web Service** instead
  (it already exposes an HTTP health endpoint on `WORKER_PORT`/`PORT` for exactly this) — but
  free Web Services on Render sleep after ~15 minutes with no inbound HTTP traffic, which would
  stop the consumer, so you'd need an external uptime pinger (e.g. UptimeRobot, cron-job.org)
  hitting its `/` health endpoint every ~10 minutes to keep it awake. That's a real workaround
  with a real failure mode, not a clean solution — Railway (or Render's $7/mo Background Worker)
  is the more honest choice if the worker needs to run unattended.

Either platform is fine for SIH judging. A VPS (DigitalOcean, etc.) with `pm2 start
server/index.js` and `pm2 start server/worker.js` works the same way if you'd rather run it
yourself — and is the easiest place to just run the broker (Redpanda via Docker) alongside
both, all under your own control.

### One important caveat: the filesystem is not permanent on most free hosting

Uploaded media files still live on disk (`mediaUrl` is uploaded to Cloudinary, but a couple of
local paths are computed too). Reports themselves are safe — MongoDB is an external database,
so restarts/redeploys don't touch them — but Render's and Railway's **free** tiers use an
ephemeral filesystem for anything actually written to local disk. Not a concern for this
project's demo/judging use since no report data depends on local disk surviving a restart.
  calls the functions exported from `db.js`, so that's the only file you'd need to change.

## API reference

| Method | Route | Auth | Purpose |
|---|---|---|---|
| `GET` | `/api/reports` | none | List all reports |
| `POST` | `/api/reports` | none | Submit a citizen report (`multipart/form-data`: `category`, `description`, `city`, `state`, `lat`?, `lng`?, `media`?) |
| `PATCH` | `/api/reports/:id/status` | admin JWT | Approve (`"verified"`) or reject (`"rejected"`) a report |
| `POST` | `/api/admin/login` | none | `{ username, password }` → `{ token }` |
| `POST` | `/api/admin/ingest` | admin JWT | Manually trigger all five live ingestion jobs now — publishes candidates onto Kafka and returns a per-source breakdown of how many were queued |
| `GET` | `/api/weather?city=` | none | Live weather lookup via Open-Meteo (used by the Forecast page) |

Admin routes expect `Authorization: Bearer <token>` from the login response.

## Adding another admin account

Right now there's a single admin identity read from `.env`. If you need more than one
admin login, the simplest change is swapping the single-account check in
`server/routes/admin.js` for a small `data/admins.json` list of `{ username, passwordHash }`
— ask if you want a hand wiring that up.

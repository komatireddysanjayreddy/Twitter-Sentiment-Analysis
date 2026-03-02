# Twitter Sentiment Analysis — Real-Time Streaming Pipeline

A production-grade, end-to-end streaming pipeline that ingests live social media
data, processes sentiment with PySpark + VADER, stores results in PostgreSQL, and
visualizes them through a Next.js dashboard on Vercel and Power BI.

```
TwitterAPI.io (WebSocket)
        │
        ▼
  [Kafka Producer] ──► Kafka Topic: tweets ──► [PySpark Processor]
                                                        │
                                                        ▼ VADER NLP
                                                  PostgreSQL DB
                                                 /            \
                                          Next.js           Power BI
                                        (Vercel)           (Desktop)
```

---

## Prerequisites

| Tool | Version | Install |
|---|---|---|
| Docker Desktop | 24+ | [docker.com](https://docker.com) |
| Python | 3.10–3.11 | [python.org](https://python.org) |
| Java JDK | 11 or 17 | Required by PySpark |
| Node.js | 18+ | [nodejs.org](https://nodejs.org) |
| Vercel CLI | latest | `npm i -g vercel` |

> **Docker Compose V2**: Modern Docker Desktop uses `docker compose` (space, not hyphen).
> All commands below use the correct V2 syntax.

---

## 1. Local Setup

### Clone and configure
```bash
cd Twitter_Sentiment_analysis
cp .env.example .env
# Edit .env and fill in your credentials
```

### Start infrastructure with Docker
```bash
docker compose up -d
```

This starts:
- **Zookeeper** on port `2181`
- **Kafka** on port `9092`
- **PostgreSQL** on port `5432`
- **pgAdmin** on [http://localhost:5050](http://localhost:5050)
- **Kafka UI** on [http://localhost:8080](http://localhost:8080)

Wait ~30 seconds for services to be healthy, then verify:
```bash
docker compose ps
```

### Initialize the database schema
Schema auto-loads via `docker-entrypoint-initdb.d/` on first boot.
To reload manually after a volume wipe:
```bash
docker exec -i twitter_postgres psql \
  -U sentiment_user -d sentiment_db < database/schema.sql
```

---

## 2. Running the Pipeline

### Option A — Docker (all-in-one, recommended)
```bash
# Uses mock_producer.py by default — no API key needed
docker compose up producer processor
```

To switch to live tweets:
```bash
docker compose run --rm producer python producer.py
```

### Option B — Local Python (for development / debugging)

**Install dependencies:**
```bash
pip install -r producer/requirements.txt
pip install -r processor/requirements.txt
```

**Mock producer (no API key):**
```bash
python producer/mock_producer.py
```

**Live producer:**
```bash
# Set TWITTER_API_KEY in .env first
python producer/producer.py
```

**PySpark Processor:**
```bash
python processor/processor.py
```
> Spark downloads Kafka/PostgreSQL JARs on first run (~2 min, cached after that).

---

## 3. Next.js Dashboard — Local

```bash
cd dashboard
npm install

# Create local env file
cp ../.env.example .env.local
# In .env.local make sure this line is correct:
# DATABASE_URL=postgresql://sentiment_user:sentiment_pass@localhost:5432/sentiment_db

npm run dev
```
Visit [http://localhost:3000](http://localhost:3000)

The dashboard auto-refreshes every **10 seconds** to match the Spark micro-batch interval.

---

## 4. Deploy Dashboard to Vercel

Vercel hosts only the **stateless Next.js layer**. Kafka + Spark run locally or on a cloud VM.
Vercel serverless functions **cannot reach `localhost`**, so you need a cloud PostgreSQL.

### Step 1 — Get a free cloud PostgreSQL (Neon recommended)
1. Go to [neon.tech](https://neon.tech) → sign up free
2. Create a project → copy the connection string:
   ```
   postgresql://user:pass@ep-xxx.us-east-2.aws.neon.tech/neondb?sslmode=require
   ```
3. Run the schema on it:
   ```bash
   psql "your-neon-connection-string" < database/schema.sql
   ```
4. In your local `.env`, set `POSTGRES_HOST`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`
   to the Neon values so Spark writes to the cloud DB.

### Step 2 — Deploy via Vercel CLI
```bash
cd dashboard
npm install -g vercel   # install once
vercel login
vercel --prod
```

When prompted, set this environment variable in Vercel:

| Variable | Value |
|---|---|
| `DATABASE_URL` | Your Neon/Supabase/Railway connection string |

### Step 3 — Or deploy via GitHub + Vercel Dashboard
1. Push this repo to GitHub
2. Go to [vercel.com](https://vercel.com) → **New Project** → import your repo
3. Set **Root Directory** to `dashboard`
4. Add environment variable `DATABASE_URL`
5. Click **Deploy**

### Cloud PostgreSQL options

| Provider | Free tier | Notes |
|---|---|---|
| **Neon** | Yes, 0.5 GB | Best free option, serverless-optimised |
| **Supabase** | Yes, 500 MB | Includes REST API + auth |
| **Railway** | $5/mo | Simple setup |
| **AWS RDS** | Free 12 mo | Most control |

---

## 5. Verifying Data Flow

```bash
# Check Kafka topic is receiving messages
docker exec -it twitter_kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic tweets --from-beginning --max-messages 5

# Check PostgreSQL has rows
docker exec -it twitter_postgres psql -U sentiment_user -d sentiment_db \
  -c "SELECT sentiment, COUNT(*) FROM tweet_sentiments GROUP BY 1;"

# Manually refresh materialized views if charts look empty
docker exec -it twitter_postgres psql -U sentiment_user -d sentiment_db \
  -c "SELECT refresh_materialized_views();"
```

---

## 6. Power BI

See [powerbi/README_POWERBI.md](powerbi/README_POWERBI.md) for full instructions.

**Quick connection:**
- Server: `localhost` (local Docker) or your cloud host
- Database: `sentiment_db`
- Mode: **DirectQuery** for live updates
- Tables: `v_sentiment_summary`, `mv_sentiment_per_minute`, `mv_top_hashtags`

---

## Architecture Notes

| Component | Design decision |
|---|---|
| **VADER** | Purpose-built for short/informal social text; handles slang and emojis |
| **Kafka partitions = 3** | Enables parallel PySpark tasks; partition key = tweet ID |
| **Micro-batch = 10s** | Balances freshness vs. PostgreSQL write overhead |
| **foreachBatch sink** | Enables upsert `ON CONFLICT` — JDBC connector is append-only |
| **Materialized views** | Pre-aggregate heavy queries; refresh via `refresh_materialized_views()` |
| **Next.js API proxy** | Database credentials never reach the browser |
| **Vercel** | Stateless dashboard layer only; Kafka/Spark stay in Docker/cloud VM |

---

## Troubleshooting

| Problem | Solution |
|---|---|
| `docker-compose: command not found` | Use `docker compose` (space, no hyphen) — Docker Desktop V2 |
| Kafka not ready on startup | Wait 30–45s; run `docker compose ps` to check health |
| PySpark JAR download fails | Ensure Java 11/17 is in `PATH`; check internet access |
| `JAVA_HOME` not set | Set `JAVA_HOME` to your JDK path before running `processor.py` |
| PostgreSQL auth error | Check `.env` credentials match `docker-compose.yml` defaults |
| Vercel "Can't reach database" | Use a cloud DB; add `?sslmode=require` to `DATABASE_URL` |
| Charts empty after deploy | Run `SELECT refresh_materialized_views();` on the cloud DB |
| Rate limit from TwitterAPI.io | Producer backs off 60s automatically; use `mock_producer.py` for testing |

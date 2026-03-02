# Twitter Sentiment Analysis (Streaming) — CLAUDE.md

## Project Overview

End-to-end real-time social media sentiment pipeline:
- **Ingest** live tweets via TwitterAPI.io WebSockets (or mock stream for testing)
- **Broker** messages through Apache Kafka
- **Process** with PySpark Structured Streaming + VADER NLP
- **Store** results in PostgreSQL
- **Visualize** via a Next.js dashboard (Vercel) and Power BI

---

## Project Structure

```
Twitter_Sentiment_analysis/
├── CLAUDE.md                    # This file
├── README.md                    # Full setup + Power BI guide
├── docker-compose.yml           # Kafka, Zookeeper, PostgreSQL, services
├── .env.example                 # Environment variable template
├── .gitignore
│
├── producer/
│   ├── producer.py              # Fetches tweets → publishes to Kafka
│   ├── mock_producer.py         # Mock stream for local testing
│   ├── requirements.txt
│   └── Dockerfile
│
├── processor/
│   ├── processor.py             # PySpark streaming → sentiment → PostgreSQL
│   ├── requirements.txt
│   └── Dockerfile
│
├── database/
│   └── schema.sql               # PostgreSQL table definitions
│
├── dashboard/                   # Next.js app → deployed to Vercel
│   ├── package.json
│   ├── next.config.js
│   ├── vercel.json
│   ├── pages/
│   │   ├── index.js             # Main dashboard UI
│   │   └── api/
│   │       ├── sentiments.js    # REST endpoint: recent sentiments
│   │       └── stats.js         # REST endpoint: aggregated stats
│   ├── components/
│   │   ├── SentimentChart.js    # Recharts real-time chart
│   │   ├── TweetFeed.js         # Live tweet cards
│   │   └── StatsBar.js          # Positive/Negative/Neutral counts
│   └── lib/
│       └── db.js                # PostgreSQL connection pool
│
└── powerbi/
    └── README_POWERBI.md        # How to connect Power BI to PostgreSQL
```

---

## Quick Start

### Prerequisites
- Docker Desktop (v24+) — uses `docker compose` (V2, space not hyphen)
- Python 3.10+
- Node.js 18+
- Java 11+ (for PySpark)

### 1. Clone and configure environment
```bash
cp .env.example .env
# Fill in your credentials in .env
```

### 2. Start infrastructure
```bash
docker compose up -d
# Starts: Zookeeper, Kafka, PostgreSQL, pgAdmin
```

### 3. Initialize database (only needed after a volume wipe)
```bash
docker exec -i twitter_postgres psql -U sentiment_user -d sentiment_db < database/schema.sql
```

### 4. Install Python dependencies
```bash
pip install -r producer/requirements.txt
pip install -r processor/requirements.txt
```

### 5. Start the producer
```bash
# Live mode (requires TwitterAPI.io key)
python producer/producer.py

# Mock mode (no API key needed)
python producer/mock_producer.py
```

### 6. Start the PySpark processor
```bash
python processor/processor.py
```

### 7. Run the dashboard locally
```bash
cd dashboard
npm install
cp ../.env.example .env.local   # then set DATABASE_URL inside
npm run dev
# Visit http://localhost:3000
```

### 8. Deploy dashboard to Vercel
```bash
cd dashboard
npx vercel --prod
# Set DATABASE_URL to a cloud PostgreSQL string in Vercel settings
```

---

## Environment Variables

| Variable | Description |
|---|---|
| `TWITTER_API_KEY` | TwitterAPI.io API key |
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka broker address |
| `KAFKA_TOPIC` | Topic name (default: `tweets`) |
| `POSTGRES_HOST` | PostgreSQL host |
| `POSTGRES_PORT` | PostgreSQL port (default: 5432) |
| `POSTGRES_DB` | Database name |
| `POSTGRES_USER` | Database user |
| `POSTGRES_PASSWORD` | Database password |
| `DATABASE_URL` | Full connection string for Next.js |

---

## Key Design Decisions

- **VADER** over TextBlob: Better accuracy for short, informal social text
- **Kafka** topic partitioned by hashtag for parallel processing
- **PySpark micro-batches** every 10 seconds to balance latency vs. throughput
- **Next.js API routes** act as a proxy so database credentials never reach the browser
- **Vercel** hosts only the stateless Next.js layer; stateful services run in Docker

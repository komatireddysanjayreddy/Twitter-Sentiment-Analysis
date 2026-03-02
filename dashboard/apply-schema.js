const { Pool } = require("pg");
const fs = require("fs");
const path = require("path");

const DB = "postgresql://neondb_owner:npg_c5wIWRQ6Olfd@ep-cool-heart-aepz1wsa-pooler.c-2.us-east-2.aws.neon.tech/neondb?sslmode=require&channel_binding=require";
const sql = fs.readFileSync(path.join(__dirname, "..", "database", "migrate_v2.sql"), "utf8");

(async () => {
  const pool = new Pool({ connectionString: DB, ssl: { rejectUnauthorized: false } });
  const client = await pool.connect();
  try {
    console.log("Applying schema to Neon...");
    await client.query(sql);
    const { rows } = await client.query("SELECT COUNT(*) FROM tweet_sentiments");
    const { rows: r2 } = await client.query("SELECT COUNT(*) FROM rolling_sentiment");
    const { rows: r3 } = await client.query(
      "SELECT column_name FROM information_schema.columns WHERE table_name='tweet_sentiments' AND column_name='processed_lag'"
    );
    console.log(`Done.`);
    console.log(`  tweet_sentiments : ${rows[0].count} rows`);
    console.log(`  rolling_sentiment: ${r2[0].count} rows`);
    console.log(`  processed_lag col: ${r3.length ? "EXISTS" : "MISSING — check migration"}`);

  } catch (e) { console.error(e.message); process.exit(1); }
  finally { client.release(); await pool.end(); }
})();

/**
 * lib/db.js — PostgreSQL connection pool for Next.js API routes.
 *
 * Uses a module-level singleton so Vercel serverless functions
 * reuse the pool across hot invocations rather than opening a
 * new connection on every request.
 *
 * Required env var: DATABASE_URL
 *   Format: postgresql://user:password@host:port/dbname
 */

import { Pool } from "pg";

let pool;

function getPool() {
  if (!pool) {
    if (!process.env.DATABASE_URL) {
      throw new Error(
        "DATABASE_URL environment variable is not set. " +
          "Add it to .env.local or Vercel project settings."
      );
    }

    pool = new Pool({
      connectionString: process.env.DATABASE_URL,
      max: 5,              // Vercel serverless: keep pool small
      idleTimeoutMillis: 30_000,
      connectionTimeoutMillis: 5_000,
      ssl:
        process.env.NODE_ENV === "production"
          ? { rejectUnauthorized: false }  // Required for Neon/Supabase/Railway
          : false,
    });

    pool.on("error", (err) => {
      console.error("[db] Unexpected pool error:", err.message);
    });
  }
  return pool;
}

/**
 * Run a parameterized query and return rows.
 * @param {string} text  — SQL statement
 * @param {any[]}  params — Bound parameters
 */
export async function query(text, params = []) {
  const client = await getPool().connect();
  try {
    const result = await client.query(text, params);
    return result.rows;
  } finally {
    client.release();
  }
}

// Test cookie-based auth with @the-convocation/twitter-scraper
// Run: node test-auth.js
// Set TW_AUTH_TOKEN and TW_CT0 to values from your browser (x.com DevTools → Application → Cookies)
const { Scraper, SearchMode } = require("@the-convocation/twitter-scraper");

async function main() {
  const AUTH_TOKEN = process.env.TW_AUTH_TOKEN;
  const CT0        = process.env.TW_CT0;

  if (!AUTH_TOKEN || !CT0) {
    console.log("Usage: TW_AUTH_TOKEN=<value> TW_CT0=<value> node test-auth.js");
    console.log("\nHow to get your cookies:");
    console.log("  1. Open https://x.com in Chrome (make sure you're logged in)");
    console.log("  2. Press F12 → Application tab → Cookies → https://x.com");
    console.log("  3. Copy the Value of 'auth_token'  → set as TW_AUTH_TOKEN");
    console.log("  4. Copy the Value of 'ct0'         → set as TW_CT0");
    process.exit(1);
  }

  console.log("Setting cookies...");
  const scraper = new Scraper();
  await scraper.setCookies([
    `auth_token=${AUTH_TOKEN}; Domain=.twitter.com; Path=/`,
    `ct0=${CT0}; Domain=.twitter.com; Path=/`,
  ]);

  const ok = await scraper.isLoggedIn();
  console.log("isLoggedIn:", ok);
  if (!ok) {
    console.error("Cookies are invalid or expired. Get fresh ones from your browser.");
    process.exit(1);
  }

  console.log("\nSearching #Bitcoin (5 tweets)...");
  let count = 0;
  for await (const t of scraper.searchTweets("#Bitcoin", 5, SearchMode.Latest)) {
    console.log(`  @${t.username}: ${t.text?.substring(0, 100)}`);
    if (++count >= 5) break;
  }
  console.log("\nSuccess —", count, "real tweets fetched.");
  console.log("\nSet these in Vercel Dashboard → Project Settings → Environment Variables:");
  console.log("  TWITTER_AUTH_TOKEN =", AUTH_TOKEN.substring(0, 8) + "...");
  console.log("  TWITTER_CT0        =", CT0.substring(0, 8) + "...");
}

main().catch((e) => { console.error("FATAL:", e.message); process.exit(1); });

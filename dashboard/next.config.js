/** @type {import('next').NextConfig} */
const nextConfig = {
  reactStrictMode: true,
  // pg is Node.js-only; mark it external so Next.js 14 doesn't try to bundle it.
  // (In Next.js 15+ this key moves to top-level `serverExternalPackages`)
  experimental: {
    serverComponentsExternalPackages: ["pg", "@the-convocation/twitter-scraper"],
  },
};

module.exports = nextConfig;

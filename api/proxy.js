// CORS proxy for the ADS-B feeds used by index.html.
// Usage: /api/proxy?u=<encoded upstream URL>
// Only the hosts below are allowed; everything else is rejected.
const ALLOWED = [
  'https://api.adsb.lol/',
  'https://opendata.adsb.fi/',
  'https://adsb.lol/data/traces/'   // recent trace per aircraft: last-known position for tails not currently transmitting
];

export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET');
  res.setHeader('Cache-Control', 'no-store');

  const u = req.query.u;
  if (!u || !ALLOWED.some(p => u.startsWith(p))) {
    return res.status(400).json({ error: 'u must be an allowed upstream URL' });
  }

  try {
    const upstream = await fetch(u, {
      signal: AbortSignal.timeout(14000),
      headers: { 'Accept': 'application/json', 'User-Agent': 'blade-fleet-ops/1.0' }
    });
    const body = await upstream.text();
    res.setHeader('Content-Type', 'application/json');
    return res.status(upstream.status).send(body);
  } catch (e) {
    return res.status(502).json({ error: e.message, name: e.name });
  }
}

export default async function handler(req, res) {
  const { icao24 } = req.query;
  if (!icao24) return res.status(400).json({ error: 'icao24 required' });

  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET');
  res.setHeader('Cache-Control', 's-maxage=15, stale-while-revalidate=30');

  try {
    const upstream = await fetch(
      'https://opensky-network.org/api/states/all?icao24=' + icao24,
      {
        signal: AbortSignal.timeout(14000),
        headers: {
          'User-Agent': 'Mozilla/5.0 (compatible; blade-fleet-ops/1.0)',
          'Accept': 'application/json'
        }
      }
    );
    if (!upstream.ok) {
      return res.status(upstream.status).json({ error: 'upstream ' + upstream.status });
    }
    const data = await upstream.json();
    res.status(200).json(data);
  } catch (e) {
    res.status(502).json({ error: e.message, name: e.name });
  }
}

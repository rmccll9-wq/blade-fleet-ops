export default async function handler(req, res) {
  const { icao24 } = req.query;
  if (!icao24) return res.status(400).json({ error: 'icao24 required' });

  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET');
  res.setHeader('Cache-Control', 's-maxage=15, stale-while-revalidate=30');

  try {
    const upstream = await fetch(
      'https://opensky-network.org/api/states/all?icao24=' + icao24,
      { signal: AbortSignal.timeout(14000) }
    );
    const data = await upstream.json();
    res.status(upstream.status).json(data);
  } catch (e) {
    res.status(502).json({ error: e.message });
  }
}

// POST /api/dj  →  runs the DJ crew. Keys stay here on the server, never in the browser.
import { runDjCrew } from '../lib/dj-crew.js';
import { checkLimit } from '../lib/limits.js';
import { str } from '../lib/gemini.js';

const list = (v, n, max) => (Array.isArray(v) ? v : []).map(x => str(x, max)).filter(Boolean).slice(0, n);

export default async function handler(req, res) {
  if (req.method !== 'POST') return res.status(405).json({ ok: false, message: 'Use POST' });
  const blocked = checkLimit(req, { perHour: 30, perDay: 80 });
  if (blocked) return res.status(429).json({ ok: false, message: blocked });

  const b = req.body || {};
  const request = str(b.request, 200);
  if (!request) return res.status(400).json({ ok: false, message: 'Tell me what you want to hear, like "@dj chill 90s Bollywood".' });

  try {
    const out = await runDjCrew({
      request,
      who: str(b.who, 24) || 'Someone',
      nowPlaying: str(b.nowPlaying, 120),
      recent: list(b.recent, 10, 120),
      recentIds: list(b.recentIds, 150, 11)
    });
    res.status(200).json(out);
  } catch (e) {
    console.error(e);
    const setup = /not set in Vercel/.test(e.message);
    res.status(500).json({ ok: false, message: setup ? 'Setup needed: ' + e.message + '.' : 'The DJ crew hit a problem: ' + String(e.message).slice(0, 160) });
  }
}

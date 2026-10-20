// POST /api/game  →  runs the quiz crew and returns the questions (with answers) to the room's timekeeper only.
import { runGameCrew } from '../lib/game-crew.js';
import { checkLimit } from '../lib/limits.js';
import { str } from '../lib/gemini.js';

const list = (v, n, max) => (Array.isArray(v) ? v : []).map(x => str(x, max)).filter(Boolean).slice(0, n);

export default async function handler(req, res) {
  if (req.method !== 'POST') return res.status(405).json({ ok: false, message: 'Use POST' });
  const blocked = checkLimit(req, { perHour: 20, perDay: 60 });
  if (blocked) return res.status(429).json({ ok: false, message: blocked });

  const b = req.body || {};
  try {
    const out = await runGameCrew({ songs: list(b.songs, 25, 120), players: list(b.players, 20, 24) });
    res.status(200).json(out);
  } catch (e) {
    console.error(e);
    const setup = /not set in Vercel/.test(e.message);
    res.status(500).json({ ok: false, message: setup ? 'Setup needed: ' + e.message + '.' : 'The quiz crew hit a problem: ' + String(e.message).slice(0, 160) });
  }
}

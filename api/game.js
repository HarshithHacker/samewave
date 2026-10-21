// POST /api/game  →  runs one of the game crews and returns the rounds (with answers) to the room's timekeeper only.
//   mode "quiz"  : Bollywood multiple choice quiz        (lib/game-crew.js)
//   mode "intro" : Pehchaan Kaun? guess the song clip    (lib/party-crews.js)
//   mode "emoji" : Emoji charades                        (lib/party-crews.js)
import { runGameCrew } from '../lib/game-crew.js';
import { runIntroCrew, runEmojiCrew } from '../lib/party-crews.js';
import { checkLimit } from '../lib/limits.js';
import { str } from '../lib/gemini.js';

const list = (v, n, max) => (Array.isArray(v) ? v : []).map(x => str(x, max)).filter(Boolean).slice(0, n);

export default async function handler(req, res) {
  if (req.method !== 'POST') return res.status(405).json({ ok: false, message: 'Use POST' });
  const blocked = checkLimit(req, { perHour: 20, perDay: 60 });
  if (blocked) return res.status(429).json({ ok: false, message: blocked });

  const b = req.body || {};
  const players = list(b.players, 20, 24);
  const avoid = list(b.avoid, 80, 80);
  try {
    let out;
    if (b.mode === 'intro') out = await runIntroCrew({ players, avoid });
    else if (b.mode === 'emoji') out = await runEmojiCrew({ players, avoid });
    else out = await runGameCrew({ songs: list(b.songs, 25, 120), players });
    res.status(200).json(out);
  } catch (e) {
    console.error(e);
    const setup = /not set in Vercel/.test(e.message);
    res.status(500).json({ ok: false, message: setup ? 'Setup needed: ' + e.message + '.' : 'The game crew hit a problem: ' + String(e.message).slice(0, 160) });
  }
}

/* Simple cost protection: a limit per device per hour and a daily cap.
   Counts live in the function's memory, so they reset when Vercel restarts it.
   That's fine for a friends-only app; Kanya's Supabase counters are the sturdier version. */

const hits = new Map();
let day = '', dayCount = 0;

export function checkLimit(req, { perHour = 30, perDay = 200 } = {}) {
  const ip = String(req.headers['x-forwarded-for'] || '').split(',')[0].trim() || 'unknown';
  const now = Date.now(), today = new Date().toISOString().slice(0, 10);
  if (today !== day) { day = today; dayCount = 0; hits.clear(); }
  if (dayCount >= perDay) return 'The crew is resting for today. Try again tomorrow.';
  const recent = (hits.get(ip) || []).filter(t => now - t < 3600e3);
  if (recent.length >= perHour) return 'That\'s a lot of requests. Give it a few minutes.';
  recent.push(now); hits.set(ip, recent); dayCount++;
  return null;
}

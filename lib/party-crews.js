/* Two party games, built the Week 3 Day 5 way: a LEAD agent designs the set,
   specialists work from the lead's plan (CrewAI's `context:`), and code checks
   every result before anyone sees it (Day 4: check by running, don't just trust).

   Pehchaan Kaun?  designer → (code: YouTube search for each song, in parallel) → verifier → host
   Emoji charades  designer → fact checker → host

   Memory (Day 4): the room sends `avoid`, the answers used in recent games, and
   the designer is told not to repeat them. */

import { runTask, str } from './gemini.js';
import { searchYouTube } from './youtube.js';

export const AGENTS = {
  designer: {
    role: 'the game designer for a group of Indian friends aged 20 to 22 who love Bollywood',
    goal: 'Plan a fun, fair, surprising set for tonight\'s game',
    backstory: 'You plan Bollywood party games for college friends. You know which songs and films from 2010 onwards everyone that age knows by heart, and you mix moods, years, singers and composers so no two rounds feel the same. You never invent songs or films.'
  },
  verifier: {
    role: 'a verifier who checks YouTube results',
    goal: 'Match each planned song to the right YouTube video, or reject it',
    backstory: 'You know YouTube well. You can tell an official audio, official video or lyric video from a cover, remix, slowed and reverb edit, mashup, jukebox, live show or reaction video. You reject anything you are not sure about.'
  },
  fact_checker: {
    role: 'a Bollywood fact checker',
    goal: 'Make sure every answer is a real Bollywood film or song released in 2010 or later, and every clue is fair',
    backstory: 'You checked facts at a Bollywood film magazine for years. You remove anything older than 2010, anything you are not sure is real, and any clue that is unfair or gives the answer away in words.'
  },
  host: {
    role: 'a game show host for a group of Indian friends',
    goal: 'Open the game with one short, fun line',
    backstory: 'You host quick Bollywood party games between songs. You are energetic and funny, you can drop a little filmy flair and a Hinglish word or two, and you use at most one emoji.'
  }
};

export const TASKS = {
  plan_intro: {
    description: 'Plan a "guess the song" game: players hear a few seconds of a song and type its name. Pick {count} popular Bollywood songs released in 2010 or later that Indian friends aged 20 to 22 would recognise. Make it a RANDOM mix: different years, moods (romantic, party, sad, item, Punjabi flavoured, indie style), singers and composers. No theme. Do not use any of these recent answers: {avoid}.',
    expected_output: 'JSON with "songs": each has "title" (the song name people know it by), "film", "year", "singer", "aliases" (other names or the hook line people use for it, can be empty) and "search_query" (a YouTube search that finds the official audio, like "<title> <film> official audio").',
    temperature: 0.9
  },
  verify_intro: {
    description: 'For each planned song, pick the YouTube result that is the actual song: official audio, official video or lyric video from a real label or channel. Reject covers, remixes, slowed or reverb edits, mashups, jukeboxes, live versions, reactions, instrumentals and anything not released in 2010 or later.\n\nPlanned songs with their search results:\n{candidates}',
    expected_output: 'JSON with "picks": one entry per planned song with "index" (the song number) and "id" (the chosen video id from that song\'s results, or an empty string to reject the song).',
    temperature: 0.1
  },
  plan_emoji: {
    description: 'Plan an emoji charades game: players see emojis and type what they mean. Pick {count} items, a RANDOM mix of Bollywood movies and Bollywood songs released in 2010 or later that Indian friends aged 20 to 22 would know. For each, write a clever emoji clue of 3 to 6 emojis that hints at the title (words, sounds or the story). Only emojis, never letters or numbers. Do not use any of these recent answers: {avoid}.',
    expected_output: 'JSON with "items": each has "type" ("movie" or "song"), "answer" (the title), "film" (for songs, the film it is from), "year", "emojis" (the clue), "aliases" (other accepted spellings, can be empty) and "fact" (one short fun fact shown after the answer).',
    temperature: 0.95
  },
  check_emoji: {
    description: 'Review these emoji charades items:\n{items_json}\n\nKeep only items that are real Bollywood movies or songs released in 2010 or later, where the year and film are correct and the emoji clue is fair and contains no letters or numbers. Fix a wrong year or film only if you are sure. Do not change the clues.',
    expected_output: 'JSON with "items" in the same shape, containing only the items you kept.',
    temperature: 0.2
  },
  open_game: {
    description: 'Write the opening line for a quick game of {game}. Players: {players}. The prize: the winner controls the room\'s music for the next 3 songs and nobody else can skip.',
    expected_output: 'JSON with "intro": one or two sentences, under 200 characters.'
  }
};

const STR = { type: 'STRING' };
const INTRO_PLAN = {
  type: 'OBJECT', required: ['songs'],
  properties: { songs: { type: 'ARRAY', items: { type: 'OBJECT', required: ['title', 'film', 'year', 'search_query'],
    properties: { title: STR, film: STR, year: { type: 'INTEGER' }, singer: STR, aliases: { type: 'ARRAY', items: STR }, search_query: STR } } } }
};
const PICKS = {
  type: 'OBJECT', required: ['picks'],
  properties: { picks: { type: 'ARRAY', items: { type: 'OBJECT', required: ['index', 'id'], properties: { index: { type: 'INTEGER' }, id: STR } } } }
};
const EMOJI_ITEMS = {
  type: 'OBJECT', required: ['items'],
  properties: { items: { type: 'ARRAY', items: { type: 'OBJECT', required: ['type', 'answer', 'year', 'emojis'],
    properties: { type: { type: 'STRING', enum: ['movie', 'song'] }, answer: STR, film: STR, year: { type: 'INTEGER' }, emojis: STR, aliases: { type: 'ARRAY', items: STR }, fact: STR } } } }
};
const INTRO = { type: 'OBJECT', properties: { intro: STR }, required: ['intro'] };

const MIN_YEAR = 2010;
const ROUNDS = 5;
const strList = (v, n, max) => (Array.isArray(v) ? v : []).map(x => str(x, max)).filter(Boolean).slice(0, n);
const shuffle = a => { for (let i = a.length - 1; i > 0; i--) { const j = Math.floor(Math.random() * (i + 1)); [a[i], a[j]] = [a[j], a[i]]; } return a; };
const words = s => String(s || '').toLowerCase().replace(/\(.*?\)|\[.*?\]/g, ' ').replace(/[^a-z0-9ऀ-ॿ ]+/g, ' ').split(/\s+/).filter(w => w.length > 1);

function opening(game, players) {
  return runTask({ agent: AGENTS.host, task: TASKS.open_game, schema: INTRO, inputs: { game, players: players.join(', ') || 'everyone' } })
    .then(r => str(r && r.intro, 220)).catch(() => '');
}

/* ---------------- Pehchaan Kaun? ---------------- */
export async function runIntroCrew({ players, avoid }) {
  const avoidText = avoid.length ? avoid.join('; ') : 'none';

  // 1. The lead plans the set (the host writes its line at the same time)
  const [plan, intro] = await Promise.all([
    runTask({ agent: AGENTS.designer, task: TASKS.plan_intro, schema: INTRO_PLAN, inputs: { count: ROUNDS + 2, avoid: avoidText } }),
    opening('Pehchaan Kaun? (guess the Bollywood song from a short clip)', players)
  ]);
  const avoidSet = new Set(avoid.map(a => a.toLowerCase()));
  const songs = ((plan && plan.songs) || [])
    .map(s => s && { title: str(s.title, 80), film: str(s.film, 80), year: Math.round(+s.year) || 0, singer: str(s.singer, 80), aliases: strList(s.aliases, 5, 80), query: str(s.search_query, 100) })
    .filter(s => s && s.title && s.year >= MIN_YEAR && !avoidSet.has(s.title.toLowerCase()))
    .slice(0, ROUNDS + 2);
  if (songs.length < 3) return { ok: false, message: 'The game designer came up short. Try again in a minute.' };

  // 2. Code searches YouTube for every song at once (faster than one agent searching one by one)
  const results = await Promise.all(songs.map(s => searchYouTube(s.query || (s.title + ' ' + s.film + ' official audio'), 5).catch(() => [])));
  const candidates = songs.map((s, i) => `Song ${i}: "${s.title}" from ${s.film} (${s.year})\n` +
    (results[i].length ? results[i].map(r => `  - id ${r.id}: ${r.title} [${r.channel}]`).join('\n') : '  (no results)')).join('\n\n');

  // 3. The verifier works from the plan and the search results (CrewAI: context)
  let picks = [];
  try {
    const v = await runTask({ agent: AGENTS.verifier, task: TASKS.verify_intro, schema: PICKS, inputs: { candidates } });
    picks = (v && v.picks) || [];
  } catch (e) { console.error('verifier failed', e); }

  // 4. Code checks the picks: the id must be one of that song's real results, and the title should match
  const chosen = [], used = new Set();
  songs.forEach((s, i) => {
    const p = picks.find(x => x && Math.round(+x.index) === i);
    let r = p && results[i].find(x => x.id === str(p.id, 11));
    if (!r && !p) {                                   // verifier said nothing: fall back to a title match
      const want = words(s.title);
      r = results[i].find(x => { const t = words(x.title); return want.length && want.every(w => t.includes(w)) && !/cover|remix|slowed|reverb|lofi|mashup|jukebox|live/i.test(x.title); });
    }
    if (r && !used.has(r.id)) { used.add(r.id); chosen.push({ s, r }); }
  });
  if (chosen.length < 3) return { ok: false, message: 'Couldn\'t find enough clean versions of the songs on YouTube. Try again.' };

  const rounds = shuffle(chosen).slice(0, ROUNDS).map(({ s, r }) => ({
    v: r.id,
    answer: s.title,
    answers: [s.title, ...s.aliases],
    detail: [s.film, s.year].filter(Boolean).join(', '),
    fact: [s.film && 'From ' + s.film + (s.year ? ' (' + s.year + ')' : ''), s.singer && 'sung by ' + s.singer].filter(Boolean).join(', ')
  }));
  return { ok: true, mode: 'intro', intro, rounds };
}

/* ---------------- Emoji charades ---------------- */
const LETTERS = /[A-Za-z0-9ऀ-ॿ]/;
function validItems(list, avoidSet) {
  return (Array.isArray(list) ? list : []).map(x => {
    if (!x) return null;
    const answer = str(x.answer, 80), emojis = str(x.emojis, 60).replace(/\s+/g, ' ');
    const year = Math.round(+x.year) || 0, type = x.type === 'song' ? 'song' : 'movie';
    if (!answer || year < MIN_YEAR || !emojis || LETTERS.test(emojis) || [...emojis.replace(/\s/g, '')].length < 2) return null;
    if (avoidSet.has(answer.toLowerCase())) return null;
    return { type, answer, film: str(x.film, 80), year, emojis, aliases: strList(x.aliases, 5, 80), fact: str(x.fact, 200) };
  }).filter(Boolean);
}

export async function runEmojiCrew({ players, avoid }) {
  const avoidSet = new Set(avoid.map(a => a.toLowerCase()));
  const [plan, intro] = await Promise.all([
    runTask({ agent: AGENTS.designer, task: TASKS.plan_emoji, schema: EMOJI_ITEMS, inputs: { count: ROUNDS + 3, avoid: avoid.length ? avoid.join('; ') : 'none' } }),
    opening('Emoji charades (guess the Bollywood movie or song from emojis)', players)
  ]);
  const drafted = validItems(plan && plan.items, avoidSet);
  if (drafted.length < 3) return { ok: false, message: 'The game designer came up short. Try again in a minute.' };

  let checked = drafted;
  try {
    const review = await runTask({ agent: AGENTS.fact_checker, task: TASKS.check_emoji, schema: EMOJI_ITEMS, inputs: { items_json: JSON.stringify(drafted) } });
    const kept = validItems(review && review.items, avoidSet);
    if (kept.length >= 3) checked = kept;
  } catch (e) { console.error('emoji check failed', e); }

  const rounds = shuffle(checked).slice(0, ROUNDS).map(x => ({
    emojis: x.emojis,
    label: x.type === 'song' ? 'Song' : 'Movie',
    answer: x.answer,
    answers: [x.answer, ...x.aliases],
    detail: x.type === 'song' ? [x.film, x.year].filter(Boolean).join(', ') : String(x.year),
    fact: x.fact
  }));
  return { ok: true, mode: 'emoji', intro, rounds };
}

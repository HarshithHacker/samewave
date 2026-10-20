/* The DJ crew: four agents working in order (CrewAI's Process.sequential).

   vibe_reader  → turns "@dj something calmer" into a structured brief
   song_scout   → searches YouTube with a custom tool and picks songs
   set_curator  → orders the songs so the energy flows
   host         → writes the intro line the room sees in chat

   The AGENTS and TASKS objects below are the same shape as CrewAI's
   agents.yaml and tasks.yaml. Edit the wording there to change the DJ's personality. */

import { runTask, str, clamp } from './gemini.js';
import { searchYouTube } from './youtube.js';

export const AGENTS = {
  vibe_reader: {
    role: 'the vibe reader for a group of friends listening to music together',
    goal: 'Turn a casual music request into a clear brief the rest of the crew can act on',
    backstory: 'You have read thousands of song requests at house parties and radio call-ins. You know what "something calmer" or "old Bollywood" really means, and you think about the room, not just the words.'
  },
  song_scout: {
    role: 'a song scout',
    goal: 'Find real, playable songs on YouTube that fit the brief',
    backstory: 'You dig through YouTube for a living. You prefer official audio, official music videos and lyric videos from real channels. You avoid covers, remixes, reaction videos, compilations, live versions and hour long mixes unless the request asks for them.'
  },
  set_curator: {
    role: 'a set curator',
    goal: 'Put the picks in an order that flows',
    backstory: 'You are a DJ who plans sets so the energy moves smoothly instead of jumping from a lullaby to a club banger.'
  },
  host: {
    role: 'a late night radio host',
    goal: 'Introduce the set to the room in one short, warm line',
    backstory: 'You host a radio show for friends. You are playful and brief, you speak like a person, and you use at most one emoji.'
  }
};

export const TASKS = {
  read_vibe: {
    description: 'Someone in the room typed this request: "{request}". The room is playing: {now_playing}. Recently played: {recent}.\nDecide whether this is a music request. If it is, describe the vibe and write 3 or 4 YouTube search queries that would find matching songs. Include artist or film names when you know good examples, and words like "song" or "official audio".',
    expected_output: 'A brief with is_music_request, summary (one short line), mood, era, languages, energy (low, medium or high), song_count (3 to 6, default 4) and search_queries.',
    temperature: 0.4
  },
  scout_songs: {
    description: 'Find {song_count} songs that fit the brief. Use the search_youtube tool for the search queries (you can improve them), read the results, and pick the best distinct songs. Only pick videos that appeared in your search results, and never invent a video id. Skip anything recently played: {recent}.',
    expected_output: 'Only JSON, no other text: {"picks":[{"id":"<the 11 character video id from the results>","why":"<a few words>"}]}'
  },
  order_set: {
    description: 'Put these songs in the best play order for this brief: {summary}\nSongs (id: title):\n{song_list}',
    expected_output: 'JSON with "order" (every video id above, each exactly once, in play order) and "arc" (one sentence about how the energy moves).',
    temperature: 0.3
  },
  introduce: {
    description: '{who} asked the DJ for: "{request}". Write the line the room will see in chat before this set plays. The set, in order:\n{song_list}',
    expected_output: 'JSON with "intro": one or two sentences, under 220 characters, that mention the first song.'
  }
};

// Structured output shapes (CrewAI: output_pydantic)
const BRIEF = {
  type: 'OBJECT',
  properties: {
    is_music_request: { type: 'BOOLEAN' },
    summary: { type: 'STRING' },
    mood: { type: 'STRING' },
    era: { type: 'STRING' },
    languages: { type: 'ARRAY', items: { type: 'STRING' } },
    energy: { type: 'STRING', enum: ['low', 'medium', 'high'] },
    song_count: { type: 'INTEGER' },
    search_queries: { type: 'ARRAY', items: { type: 'STRING' } }
  },
  required: ['is_music_request', 'summary', 'search_queries']
};
const ORDER = {
  type: 'OBJECT',
  properties: { order: { type: 'ARRAY', items: { type: 'STRING' } }, arc: { type: 'STRING' } },
  required: ['order']
};
const INTRO = { type: 'OBJECT', properties: { intro: { type: 'STRING' } }, required: ['intro'] };

const MAX_SEARCHES = 4;

export async function runDjCrew({ request, who, nowPlaying, recent, recentIds }) {
  const recentText = recent.length ? recent.join('; ') : 'nothing yet';
  const skip = new Set(recentIds);

  // 1. Vibe reader. It also acts as the guardrail: non music requests stop here, before any searching.
  const brief = await runTask({
    agent: AGENTS.vibe_reader, task: TASKS.read_vibe, schema: BRIEF,
    inputs: { request, now_playing: nowPlaying || 'nothing', recent: recentText }
  });
  if (!brief || !brief.is_music_request) {
    return { ok: false, message: 'I only take music requests. Try something like "@dj upbeat Punjabi party songs".' };
  }
  const count = clamp(brief.song_count, 3, 6, 4);
  const queries = (Array.isArray(brief.search_queries) ? brief.search_queries : []).map(q => str(q, 100)).filter(Boolean).slice(0, 4);
  if (!queries.length) queries.push(request);

  // 2. Song scout, with a custom tool. Every result is remembered, so code can check the picks afterwards.
  const found = new Map(), searched = new Set();
  let searches = 0;
  const searchTool = {
    declaration: {
      name: 'search_youtube',
      description: 'Search YouTube for videos. Returns up to 6 results, each with id, title and channel.',
      parameters: { type: 'OBJECT', properties: { query: { type: 'STRING', description: 'What to search for' } }, required: ['query'] }
    },
    run: async ({ query }) => {
      if (searches >= MAX_SEARCHES) return { error: 'Search limit reached. Pick from the results you already have.' };
      searches++;
      searched.add(String(query || request).toLowerCase().trim());
      const results = await searchYouTube(String(query || request));
      results.forEach(r => found.set(r.id, r));
      return results;
    }
  };
  let scout = null;
  try {
    scout = await runTask({
      agent: AGENTS.song_scout, task: TASKS.scout_songs, tools: [searchTool], schema: true, maxTurns: 6,
      inputs: { song_count: count, recent: recentText },
      context: 'Brief: ' + JSON.stringify({ summary: brief.summary, mood: brief.mood, era: brief.era, languages: brief.languages, energy: brief.energy, search_queries: queries })
    });
  } catch (e) {
    if (/not set in Vercel/.test(e.message)) throw e;
    console.error('scout failed', e);
  }

  // Facts stay in code: only keep picks that really came back from YouTube, with no repeats
  const used = new Set();
  let picks = ((scout && scout.picks) || [])
    .map(p => p && str(p.id, 11))
    .filter(id => id && found.has(id) && !skip.has(id) && !used.has(id) && used.add(id));

  // Safety net: if the scout came up short, take the top result for each of the brief's queries
  for (const q of queries) {
    if (picks.length >= count || searches >= MAX_SEARCHES) break;
    if (searched.has(q.toLowerCase().trim())) continue;
    searches++;
    const results = await searchYouTube(q);
    results.forEach(r => found.set(r.id, r));
    const first = results.find(r => !used.has(r.id) && !skip.has(r.id));
    if (first) { used.add(first.id); picks.push(first.id); }
  }
  picks = picks.slice(0, count);
  if (!picks.length) return { ok: false, message: 'I couldn\'t find songs for that. Try different words.' };

  const songList = picks.map(id => `${id}: ${found.get(id).title} (${found.get(id).channel})`).join('\n');

  // 3. Set curator. Code makes sure the order uses exactly the picked songs.
  let order = picks;
  if (picks.length > 1) {
    try {
      const res = await runTask({ agent: AGENTS.set_curator, task: TASKS.order_set, schema: ORDER, inputs: { summary: brief.summary, song_list: songList } });
      const seen = new Set();
      const proposed = ((res && res.order) || []).map(id => str(id, 11)).filter(id => picks.includes(id) && !seen.has(id) && seen.add(id));
      order = [...proposed, ...picks.filter(id => !seen.has(id))];
    } catch (e) { console.error('curator failed', e); }
  }

  // 4. Host
  const orderedList = order.map((id, i) => `${i + 1}. ${found.get(id).title}`).join('\n');
  let intro = '';
  try {
    const res = await runTask({ agent: AGENTS.host, task: TASKS.introduce, schema: INTRO, inputs: { who, request, song_list: orderedList } });
    intro = str(res && res.intro, 240);
  } catch (e) { console.error('host failed', e); }
  if (!intro) intro = 'Coming right up, starting with ' + found.get(order[0]).title + '.';

  return {
    ok: true,
    brief: { summary: str(brief.summary, 140), mood: str(brief.mood, 60), energy: str(brief.energy, 10) },
    songs: order.map(id => ({ v: id, title: found.get(id).title })),
    intro,
    searches
  };
}

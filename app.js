/* Samewave: listen to YouTube together, in sync.

   How the room works
   - One phone in the room is the "timekeeper". It plays the real clock and owns
     the queue. Nobody sees this in the UI; everyone has the same controls.
   - Everyone else sends requests (play, pause, skip, add, chat) and follows the
     timekeeper's position.
   - If the timekeeper leaves, the person who joined next takes over, so the room
     keeps going.
   - Messages travel through three free public MQTT relay servers. Everything is
     sent to all three, so one blocked server doesn't matter. */

const $ = id => document.getElementById(id);
const BROKERS = [
  'wss://broker.hivemq.com:8884/mqtt',
  'wss://broker.emqx.io:8084/mqtt',
  'wss://test.mosquitto.org:8081'
];
const NS = 'samewave-hy/v3/';
const HOST_TIMEOUT = 12000;         // silence before someone else takes over
const MEMBER_TIMEOUT = 60000;       // silence before someone counts as gone (backgrounded phones check in rarely)
const QUEUE_MAX = 100;

const rid = n => { let s = ''; while (s.length < n) s += Math.random().toString(36).slice(2); return s.slice(0, n); };
const clean = (s, max) => String(s == null ? '' : s).replace(/[\u0000-\u001f\u007f]/g, ' ').trim().slice(0, max);
const okV = v => typeof v === 'string' && /^[\w-]{11}$/.test(v);
const fmt = t => { t = Math.max(0, Math.floor(t || 0)); return Math.floor(t / 60) + ':' + String(t % 60).padStart(2, '0'); };
const store = {
  get(k) { try { return localStorage.getItem(k); } catch (e) { return null; } },
  set(k, v) { try { localStorage.setItem(k, v); } catch (e) {} }
};
const me = (() => {
  try { let id = sessionStorage.getItem('sw_id'); if (!id) { id = rid(8); sessionStorage.setItem('sw_id', id); } return id; }
  catch (e) { return rid(8); }
})();

let myName = '', code = null, role = null, joinedAt = 0, inRoom = false;
let clients = [];
let yt = null, ytReady = false, probe = null, probeReady = false, loadedId = null, unlocked = false;
let room = null;                    // shared room state, owned by the timekeeper
let clockOff = 0, bestRtt = Infinity, lastStateSeen = 0;
let chat = [];
const seenMsg = new Set(), seenCtl = new Set();
const knownIds = new Set();          // everyone who has been in this room
// timekeeper only
let members = {}, played = [], radioFor = null, chatlogTimer = null;
let probeJobs = [], probeBusy = null, titleBusy = false;
let joinTimer = null, seeking = false;

/* ---------------- relay ---------------- */
function connectAll(asHost) {
  const will = asHost ? { topic: NS + code + '/state', payload: '', retain: true, qos: 0 } : undefined;
  clients = BROKERS.map(url => {
    let c;
    try {
      c = mqtt.connect(url, { connectTimeout: 8000, reconnectPeriod: 4000, clean: true, clientId: 'sw_' + me + '_' + rid(4), will });
    } catch (e) { return null; }
    c.on('error', () => {});
    c.on('connect', () => { c.subscribe(NS + code + '/#'); renderStatus(); });
    c.on('close', renderStatus);
    c.on('message', (topic, buf) => onMsg(topic.slice((NS + code + '/').length), buf.toString()));
    return c;
  }).filter(Boolean);
}
const anyConnected = () => clients.some(c => c.connected);
function pub(sub, obj, retain) {
  const payload = obj == null ? '' : JSON.stringify(obj);
  clients.forEach(c => { if (c.connected) c.publish(NS + code + '/' + sub, payload, { retain: !!retain, qos: 0 }); });
}
function closeAll() { clients.forEach(c => { try { c.end(true); } catch (e) {} }); clients = []; }

function onMsg(sub, raw) {
  if (sub === 'state') return onState(raw);
  if (!raw) return;
  let d; try { d = JSON.parse(raw); } catch (e) { return; }
  if (!d || typeof d !== 'object') return;
  if (sub === 'chat') return onChat(d);
  if (sub === 'chatlog') { if (Array.isArray(d)) d.slice(-40).forEach(onChat); return; }
  if (role === 'host') {
    if (sub === 'ping' && typeof d.id === 'string') return onPing(d);
    if (sub === 'ctl') return handleCtl(d);
    return;
  }
  if (sub === 'pong/' + me && typeof d.c === 'number' && typeof d.h === 'number') {
    const now = Date.now(), rtt = now - d.c;
    if (rtt < 3000 && rtt <= bestRtt * 1.5) { bestRtt = Math.min(bestRtt, rtt); clockOff = d.h + rtt / 2 - now; }
  }
}

/* ---------------- shared state ---------------- */
function sanitizeRoom(d) {
  return {
    epoch: +d.epoch || 0, hostId: clean(d.hostId, 20), at: +d.at || 0, left: !!d.left,
    videoId: okV(d.videoId) ? d.videoId : null, title: clean(d.title, 120), by: clean(d.by, 24),
    playing: !!d.playing, pos: +d.pos || 0, dur: +d.dur || 0,
    queue: (Array.isArray(d.queue) ? d.queue : []).filter(q => q && okV(q.v)).slice(0, QUEUE_MAX)
      .map(q => ({ qid: clean(q.qid, 12), v: q.v, title: clean(q.title, 120), by: clean(q.by, 24), radio: !!q.radio })),
    members: (Array.isArray(d.members) ? d.members : []).slice(0, 50)
      .map(m => ({ id: clean(m.id, 20), n: clean(m.n, 24), j: +m.j || 0 })),
    lock: sanitizeLock(d.lock),
    game: sanitizeGame(d.game)
  };
}
function sanitizeLock(l) {
  if (!l || typeof l !== 'object' || !clean(l.id, 20)) return null;
  return { id: clean(l.id, 20), n: clean(l.n, 24) || 'Someone', played: Math.max(0, +l.played || 0), max: Math.min(10, Math.max(1, +l.max || 3)) };
}
const GAME_STAGES = ['loading', 'q', 'reveal', 'over'];
function sanitizeGame(g) {
  if (!g || typeof g !== 'object' || !GAME_STAGES.includes(g.stage)) return null;
  const arr = a => (Array.isArray(a) ? a : []);
  return {
    gid: clean(g.gid, 12), stage: g.stage, idx: Math.max(0, +g.idx || 0), total: Math.max(0, +g.total || 0),
    mode: ['quiz', 'intro', 'emoji'].includes(g.mode) ? g.mode : 'quiz',
    q: clean(g.q, 200), options: arr(g.options).slice(0, 4).map(o => clean(o, 80)),
    emoji: clean(g.emoji, 60), label: clean(g.label, 12), hint: clean(g.hint, 120), answer: clean(g.answer, 160),
    clip: g.clip && okV(g.clip.v) ? { key: clean(g.clip.key, 8), v: g.clip.v, start: Math.max(0, +g.clip.start || 0), len: Math.min(10, Math.max(1, +g.clip.len || 3)), at: +g.clip.at || 0 } : null,
    endsAt: +g.endsAt || 0, roundMs: Math.min(60000, Math.max(5000, +g.roundMs || 15000)),
    answerIdx: Number.isInteger(g.answerIdx) ? g.answerIdx : -1, fact: clean(g.fact, 200),
    answered: arr(g.answered).slice(0, 50).map(x => clean(x, 20)),
    right: arr(g.right).slice(0, 30).map(x => clean(x, 24)).filter(Boolean),
    scores: arr(g.scores).slice(0, 50).filter(Boolean).map(x => ({ id: clean(x.id, 20), n: clean(x.n, 24) || 'Someone', pts: Math.max(0, +x.pts || 0) })),
    winner: g.winner && g.winner.id ? { id: clean(g.winner.id, 20), n: clean(g.winner.n, 24) || 'Someone' } : null
  };
}

function onState(raw) {
  if (!raw) {                                     // timekeeper dropped off without saying goodbye
    if (role === 'listener' && room) lastStateSeen = Math.min(lastStateSeen, Date.now() - HOST_TIMEOUT);
    return;
  }
  let d; try { d = JSON.parse(raw); } catch (e) { return; }
  if (!d || typeof d.at !== 'number' || typeof d.hostId !== 'string') return;
  d = sanitizeRoom(d);
  if (role === 'host') {
    if (d.hostId !== me && (d.epoch > room.epoch || (d.epoch === room.epoch && d.hostId < me))) stepDown(d);
    return;
  }
  if (role !== 'listener') return;
  if (room && (d.epoch < room.epoch || (d.epoch === room.epoch && d.at <= room.at))) return;
  if (d.left) {
    if (room) { room = d; lastStateSeen = Date.now() - HOST_TIMEOUT; renderStatus(); }
    return;
  }
  if (room && room.hostId !== d.hostId) bestRtt = Infinity;
  room = d; lastStateSeen = Date.now();
  room.members.forEach(m => knownIds.add(m.id));
  if (!inRoom) enterRoom();
  renderAll(); applyVideo(); sync();
}

function expectedPos() {
  if (!room) return 0;
  if (!room.playing) return room.pos;
  const now = role === 'host' ? Date.now() : Date.now() + clockOff;
  return room.pos + (now - room.at) / 1000;
}

/* ---------------- timekeeper ---------------- */
function memberList() {
  const list = [{ id: me, n: myName, j: joinedAt }];
  Object.values(members).forEach(m => list.push({ id: m.id, n: m.n, j: m.j }));
  return list.sort((a, b) => a.j - b.j || (a.id < b.id ? -1 : 1));
}
function readPlayer() {
  if (!ytReady || !room.videoId || loadedId !== room.videoId) return;
  const s = yt.getPlayerState();
  room.playing = s === YT.PlayerState.PLAYING || s === YT.PlayerState.BUFFERING;
  room.pos = yt.getCurrentTime() || 0;
  room.dur = yt.getDuration() || room.dur || 0;
  const d = yt.getVideoData ? yt.getVideoData() : null;
  if (d && d.title && d.video_id === room.videoId) room.title = clean(d.title, 120);
}
function pushState() {
  if (role !== 'host' || !room) return;
  readPlayer();
  room.at = Date.now();
  room.hostId = me;
  room.members = memberList();
  pub('state', room, true);
  renderAll();
}
function onPing(d) {
  const id = clean(d.id, 20), now = Date.now();
  if (!id || id === me) return;
  const isNew = !members[id];
  members[id] = { id, n: clean(d.n, 24) || 'Someone', j: +d.j || now, seen: now };
  pub('pong/' + id, { c: d.c, h: now });
  if (isNew) {
    if (!knownIds.has(id)) sys(members[id].n + ' joined');
    knownIds.add(id);
    pushState();
  }
}
function expireMembers() {
  const now = Date.now(); let changed = false;
  for (const id in members) {
    if (now - members[id].seen > MEMBER_TIMEOUT) {
      sys(members[id].n + ' left'); knownIds.delete(id);
      if (room.lock && room.lock.id === id) { room.lock = null; sys('🔓 The lock is off because ' + members[id].n + ' left.'); }
      delete members[id]; changed = true;
    }
  }
  if (changed) pushState();
}

function handleCtl(d) {
  const id = clean(d.id, 16);
  if (!id || seenCtl.has(id)) return;           // the same request arrives once per relay server
  seenCtl.add(id);
  const who = clean(d.n, 24) || 'Someone';
  const from = clean(d.from, 20);
  if (d.t === 'answer') return onAnswer(d, from, who);
  if (d.t === 'guess') return onGuess(d, from, who);
  if (introRunning() && ['play', 'pause', 'seek', 'next', 'prev', 'playnow'].includes(d.t)) {
    if (Date.now() - lastLockNote > 4000) { lastLockNote = Date.now(); sys('The music is paused while Pehchaan Kaun? is on. It comes back after the game.'); }
    return;
  }
  if (room.lock && from !== room.lock.id && LOCKED.includes(d.t)) {   // only the game winner can change things
    if (d.t !== 'seek') lockedNotice();
    return;
  }
  switch (d.t) {
    case 'game': startGame(who, d.mode); break;
    case 'play':
      if (!room.videoId) { if (room.queue.length) advance(); }
      else if (ytReady) yt.playVideo();
      break;
    case 'pause': if (ytReady) yt.pauseVideo(); break;
    case 'seek':
      if (ytReady && room.videoId && isFinite(+d.pos)) { yt.seekTo(Math.max(0, +d.pos), true); setTimeout(pushState, 300); }
      break;
    case 'next':
      if (room.videoId || room.queue.length) { sys(who + ' skipped'); advance(); }
      break;
    case 'prev': goPrev(); break;
    case 'add': addLink(d.link, who, !!(room.lock && from === room.lock.id)); break;
    case 'remove': room.queue = room.queue.filter(q => q.qid !== d.qid); pushState(); break;
    case 'playnow': {
      const i = room.queue.findIndex(q => q.qid === d.qid);
      if (i < 0) break;
      const [it] = room.queue.splice(i, 1);
      if (room.videoId) { played.push({ v: room.videoId, title: room.title, by: room.by }); played = played.slice(-30); }
      sys(who + ' played ' + (it.title || 'a song'));
      playItem(it);
      break;
    }
    case 'up': {
      const i = room.queue.findIndex(q => q.qid === d.qid);
      if (i > 0) { const [it] = room.queue.splice(i, 1); room.queue.splice(i - 1, 0, it); pushState(); }
      break;
    }
    case 'move': {
      if (moveInQueue(room.queue, d.qid, +d.to)) pushState();
      break;
    }
  }
}

function addLink(link, who, front) {
  if (!link || typeof link !== 'object') return;
  const v = okV(link.v) ? link.v : null;
  const list = typeof link.list === 'string' && /^[\w-]{10,64}$/.test(link.list) ? link.list : null;
  if (list) { probeRun({ kind: 'list', list, v, who, front }); return; }
  if (v) { insertItems([{ qid: rid(8), v, title: '', by: who }], front); sys(who + ' added a song'); }
}
function insertItems(items, front) {
  if (front) insertFront(items, true);
  else {
    const i = room.queue.findIndex(q => q.radio);   // people's picks go ahead of radio picks
    if (i < 0) room.queue.push(...items); else room.queue.splice(i, 0, ...items);
  }
  room.queue = room.queue.slice(0, QUEUE_MAX);
  if (!room.videoId) advance(); else pushState();
  fetchTitles();
}
function advance() {
  releaseLockIfDone();
  if (room.videoId) { played.push({ v: room.videoId, title: room.title, by: room.by }); played = played.slice(-30); }
  const item = room.queue.shift();
  if (!item) {
    room.videoId = null; room.title = ''; room.by = ''; room.playing = false; room.pos = 0; room.dur = 0;
    loadedId = null; if (ytReady) yt.stopVideo();
    sys('Queue finished. Add a song to keep going.');
    pushState();
    return;
  }
  playItem(item);
}
function playItem(item) {
  releaseLockIfDone();                            // the winner's songs are done
  if (room.lock) room.lock.played++;
  room.videoId = item.v; room.title = item.title || ''; room.by = item.radio ? 'Radio' : item.by;
  room.pos = 0; room.dur = 0; room.playing = true;
  loadedId = item.v;
  if (ytReady) yt.loadVideoById(item.v);
  room.at = Date.now(); room.members = memberList(); room.hostId = me;
  pub('state', room, true);                      // skip readPlayer: the player still reports the old song for a moment
  renderAll();
}
function goPrev() {
  if (!ytReady) return;
  if ((yt.getCurrentTime() || 0) > 4 || !played.length) { yt.seekTo(0, true); setTimeout(pushState, 300); return; }
  const prev = played.pop();
  if (room.videoId) room.queue.unshift({ qid: rid(8), v: room.videoId, title: room.title, by: room.by, radio: room.by === 'Radio' });
  playItem({ v: prev.v, title: prev.title, by: prev.by, radio: prev.by === 'Radio' });
}

/* Radio: when the queue is empty, quietly fetch YouTube's mix for the current song
   in a hidden second player and add the next songs from it. */
function maybeRadio() {
  if (role !== 'host' || !room.videoId || room.queue.length || radioFor === room.videoId || !probeReady) return;
  radioFor = room.videoId;
  probeRun({ kind: 'radio', seed: room.videoId });
}
function probeRun(job) { probeJobs.push(job); probeNext(); }
function probeNext() {
  if (probeBusy || !probeReady || !probeJobs.length) return;
  const job = probeBusy = probeJobs.shift();
  job.timer = setTimeout(() => probeDone(job, []), 8000);
  try { probe.cuePlaylist({ listType: 'playlist', list: job.kind === 'radio' ? 'RD' + job.seed : job.list, index: 0 }); }
  catch (e) { probeDone(job, []); }
}
function onProbeState(e) {
  const job = probeBusy;
  if (!job || e.data !== YT.PlayerState.CUED) return;
  const ids = (probe.getPlaylist && probe.getPlaylist()) || [];
  if (ids.length) probeDone(job, ids);
  else setTimeout(() => { if (probeBusy === job) probeDone(job, (probe.getPlaylist && probe.getPlaylist()) || []); }, 1200);
}
function probeDone(job, ids) {
  if (probeBusy !== job) return;
  clearTimeout(job.timer); probeBusy = null;
  ids = ids.filter(okV);
  if (role === 'host') {
    if (job.kind === 'radio') {
      const recent = new Set([job.seed, room.videoId, ...played.map(h => h.v), ...room.queue.map(q => q.v)]);
      const picks = ids.filter(v => !recent.has(v)).slice(0, 12);
      if (picks.length) {
        room.queue.push(...picks.map(v => ({ qid: rid(8), v, title: '', by: 'Radio', radio: true })));
        sys('Radio added ' + picks.length + ' similar songs');
        pushState(); fetchTitles();
      }
    } else {
      let list = ids;
      if (job.v) { const i = ids.indexOf(job.v); if (i > 0) list = ids.slice(i); }
      if (list.length) {
        insertItems(list.slice(0, QUEUE_MAX).map(v => ({ qid: rid(8), v, title: '', by: job.who })), job.front);
        sys(job.who + ' added a playlist (' + Math.min(list.length, QUEUE_MAX) + ' songs)');
      } else {
        sys('Couldn\'t open that playlist. It has to be public or unlisted.');
      }
    }
  }
  setTimeout(probeNext, 300);
}

async function getTitle(v) {
  const u = encodeURIComponent('https://www.youtube.com/watch?v=' + v);
  for (const url of ['https://www.youtube.com/oembed?format=json&url=' + u, 'https://noembed.com/embed?url=' + u]) {
    try { const r = await fetch(url); if (r.ok) { const j = await r.json(); if (j && j.title) return clean(j.title, 120); } } catch (e) {}
  }
  return '';
}
async function fetchTitles() {
  if (titleBusy || role !== 'host') return;
  titleBusy = true;
  try {
    let n = 0, item;
    while (role === 'host' && (item = room.queue.find(q => !q.title && !q.tried))) {
      item.tried = true;
      item.title = await getTitle(item.v);
      if (++n % 4 === 0) pushState();
    }
    if (n) pushState();
  } finally { titleBusy = false; }
}

/* ---------------- taking over / stepping down ---------------- */
function checkSuccession() {
  if (role !== 'listener' || !room) return;
  const silent = Date.now() - lastStateSeen;
  if (silent < HOST_TIMEOUT) return;
  const cands = (room.members || []).filter(m => m.id !== room.hostId).sort((a, b) => a.j - b.j || (a.id < b.id ? -1 : 1));
  const idx = Math.floor((silent - HOST_TIMEOUT) / 4000);   // if the first in line is gone too, the next one tries
  if (idx < cands.length && cands[idx].id === me) takeOver();
}
function takeOver() {
  const old = room, now = Date.now(), pos = expectedPos();
  role = 'host';
  members = {};
  (old.members || []).forEach(m => { if (m.id !== me) { members[m.id] = { id: m.id, n: m.n, j: m.j, seen: now }; knownIds.add(m.id); } });
  room = { ...old, epoch: old.epoch + 1, hostId: me, left: false, at: now, pos };
  if (room.game) {
    const live = room.game.stage !== 'over';
    room.game = null;
    if (live) setTimeout(() => sys('The game was cancelled because the room\'s timekeeper changed. Start a new one any time.'), 1500);
  }
  played = []; radioFor = null; bestRtt = Infinity; clockOff = 0;
  if (ytReady && room.videoId && loadedId !== room.videoId) { loadedId = room.videoId; yt.loadVideoById({ videoId: room.videoId, startSeconds: pos }); }
  closeAll(); connectAll(true);
  const w = setInterval(() => { if (anyConnected()) { clearInterval(w); pushState(); fetchTitles(); } }, 200);
  setTimeout(() => clearInterval(w), 15000);
  renderAll();
}
function stepDown(d) {
  role = 'listener';
  closeAll(); connectAll(false);
  room = d; lastStateSeen = Date.now(); bestRtt = Infinity;
  renderAll(); applyVideo();
}

/* ---------------- following along ---------------- */
function applyVideo() {
  if (!ytReady || !room || role !== 'listener' || clipActive()) return;
  if (!room.videoId) { if (loadedId) { loadedId = null; yt.stopVideo(); } return; }
  if (room.videoId === loadedId) return;
  loadedId = room.videoId;
  if (unlocked) yt.loadVideoById({ videoId: room.videoId, startSeconds: Math.max(0, expectedPos()) });
  else yt.cueVideoById(room.videoId);
}
function sync() {
  if (clipActive()) return;
  if (role !== 'listener' || !room || !room.videoId || !ytReady || !unlocked || loadedId !== room.videoId) return;
  const exp = expectedPos(), cur = yt.getCurrentTime() || 0, ps = yt.getPlayerState();
  if (ps === YT.PlayerState.ENDED) return;       // wait for the room to move to the next song
  if (!room.playing) {
    if (ps === YT.PlayerState.PLAYING) yt.pauseVideo();
    if (Math.abs(cur - exp) > 0.5) yt.seekTo(exp, true);
    showOff(0); return;
  }
  if (room.dur && exp > room.dur - 0.5) return;
  if (ps !== YT.PlayerState.PLAYING && ps !== YT.PlayerState.BUFFERING) yt.playVideo();
  const d = cur - exp;
  if (Math.abs(d) > 0.4 && ps === YT.PlayerState.PLAYING) yt.seekTo(exp + 0.25, true);
  showOff(d);
}
function showOff(d) {
  const ms = Math.round(d * 1000), el = $('off');
  el.textContent = (ms >= 0 ? '+' : '−') + Math.abs(ms) + ' ms';
  el.classList.toggle('far', Math.abs(ms) > 300);
}
function ping() { if (role === 'listener') pub('ping', { id: me, n: myName, j: joinedAt, c: Date.now() }); }

/* ---------------- chat ---------------- */
function onChat(d) {
  const id = clean(d.id, 16);
  if (!id || seenMsg.has(id)) return;
  seenMsg.add(id);
  const m = { id, at: +d.at || Date.now(), sys: !!d.sys, bot: clean(d.bot, 16), from: clean(d.from, 20), n: clean(d.n, 24) || 'Someone', text: clean(d.text, 300), ann: !!d.ann };
  if (!m.text) return;
  m.dj = !!d.dj || m.bot === 'DJ' || (!m.sys && !m.bot && /^@dj\b/i.test(m.text));
  chat.push(m);
  chat.sort((a, b) => a.at - b.at);
  if (chat.length > 100) chat = chat.slice(-100);
  renderChat(); renderDj();
  if (role === 'host') {
    scheduleChatlog();
    if (!m.sys && !m.bot && Date.now() - m.at < 60000) {
      if (m.dj) djRequest(m);
      else if (/^@game\b/i.test(m.text)) {
        const w = m.text.toLowerCase();
        const mode = /emoji|charade/.test(w) ? 'emoji' : /song|intro|pehchaan|guess/.test(w) ? 'intro' : 'quiz';
        handleCtl({ id: 'g' + m.id, n: m.n, from: m.from, t: 'game', mode });
      }
    }
  }
}
function scheduleChatlog() {                     // so people who join later see recent messages
  if (chatlogTimer) return;
  chatlogTimer = setTimeout(() => {
    chatlogTimer = null;
    if (role === 'host') pub('chatlog', chat.slice(-40).map(m => ({ id: m.id, at: m.at, sys: m.sys, bot: m.bot, n: m.n, text: m.text, dj: m.dj, ann: m.ann })), true);
  }, 1000);
}
function sendChat(text, dj) {
  const msg = { id: rid(12), from: me, n: myName, dj: !!dj, text: clean(text, 300), at: Date.now() + (role === 'listener' ? clockOff : 0) };
  if (!msg.text) return;
  pub('chat', msg); onChat(msg);
}
function sys(text, dj) {
  if (role !== 'host') return;
  const msg = { id: rid(12), sys: true, dj: !!dj, text, at: Date.now() };
  pub('chat', msg); onChat(msg);
}

/* ---------------- the winner's lock ---------------- */
const LOCKED = ['play', 'pause', 'seek', 'next', 'prev', 'add', 'remove', 'up', 'move', 'playnow', 'game'];
const lockedOut = () => !!(room && room.lock && room.lock.id !== me);
let lastLockNote = 0;
function lockedNotice() {
  if (Date.now() - lastLockNote < 4000) return;
  lastLockNote = Date.now();
  sys('🔒 ' + room.lock.n + ' won the game, so only ' + room.lock.n + ' can change the music right now.');
}
// DJ picks go to the front of the queue. The game winner's picks go ahead of everything.
function insertFront(items, winnerPick) {
  let i = 0;
  while (i < room.queue.length && (room.queue[i].winnerPick || (!winnerPick && room.queue[i].front))) i++;
  items.forEach(x => { if (winnerPick) x.winnerPick = true; else x.front = true; });
  room.queue.splice(i, 0, ...items);
}
function releaseLockIfDone() {
  if (!room.lock || room.lock.played < room.lock.max) return;
  sys('🔓 ' + room.lock.n + '\'s turn is over. Everyone can control the music again.');
  room.lock = null;
  room.queue.forEach(q => { delete q.winnerPick; });
}
function bot(text, name, ann) {                  // a message from one of the AI crews
  if (role !== 'host') return;
  const msg = { id: rid(12), bot: name, n: name, ann: !!ann, text: clean(text, 300), at: Date.now() };
  pub('chat', msg); onChat(msg);
}

/* ---------------- @dj: the DJ crew ---------------- */
let djBusy = false;
async function djRequest(m) {
  if (room.lock && m.from !== room.lock.id) { lockedNotice(); return; }
  const request = m.text.replace(/^@dj\b[:,]?\s*/i, '').trim();
  if (!request) { bot('Tell me what you want to hear, like "chill 90s Bollywood".', 'DJ'); return; }
  if (djBusy) { bot('Still working on the last request. One at a time!', 'DJ'); return; }
  djBusy = true;
  sys('🎧 On it: reading the vibe and digging for songs…', true);
  try {
    const res = await fetch('/api/dj', {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        request, who: m.n, nowPlaying: room.title || '',
        recent: [...played.slice(-8).map(p => p.title), room.title].filter(Boolean),
        recentIds: [...played.map(p => p.v), room.videoId, ...room.queue.map(q => q.v)].filter(Boolean)
      })
    });
    const out = await res.json().catch(() => ({ ok: false, message: 'The DJ crew sent back something I couldn\'t read.' }));
    if (role !== 'host') return;
    if (!out.ok) { bot(out.message || 'I couldn\'t do that one. Try different words.', 'DJ'); return; }
    const items = (Array.isArray(out.songs) ? out.songs : []).filter(x => x && okV(x.v)).slice(0, 8)
      .map(x => ({ qid: rid(8), v: x.v, title: clean(x.title, 120), by: 'DJ' }));
    if (!items.length) { bot('No playable songs came back. Try different words.', 'DJ'); return; }
    if (out.brief && out.brief.summary) sys('Vibe: ' + clean(out.brief.summary, 140), true);
    bot('🎧 ' + (out.intro || ('Coming up: ' + items[0].title)), 'DJ', true);   // also shown in the main chat
    insertFront(items, !!(room.lock && m.from === room.lock.id));
    room.queue = room.queue.slice(0, QUEUE_MAX);
    if (!room.videoId) advance(); else pushState();
  } catch (e) {
    bot('I couldn\'t reach the DJ crew. Check the connection and try again.', 'DJ');
  } finally { djBusy = false; }
}

/* ---------------- games ----------------
   Three games, picked from the Game tab:
     quiz   Bollywood quiz, multiple choice           (api/game.js → lib/game-crew.js)
     intro  Pehchaan Kaun? guess the song from a clip  (lib/party-crews.js)
     emoji  Emoji charades                            (lib/party-crews.js)
   The timekeeper keeps every answer to itself. Everyone else only sees the current
   round, and its answer at the reveal, so nobody can peek. Guesses are checked here
   in code, with spelling tolerance (Week 3 Day 4: check results by running code). */
const GAMES = {
  quiz:  { name: 'Bollywood quiz', bot: 'Quizmaster', loading: 'The quiz crew is writing questions…', rounds: 3, ms: 15000 },
  intro: { name: 'Pehchaan Kaun?', bot: 'Host', loading: 'The game crew is picking songs and checking them on YouTube…', rounds: 5, ms: 20000 },
  emoji: { name: 'Emoji charades', bot: 'Host', loading: 'The game crew is cooking up emoji clues…', rounds: 5, ms: 30000 }
};
const REVEAL_MS = 6000, LOCK_SONGS = 3;
const CLIP_SECS = [6, 6, 4, 4, 2];               // clips get shorter every round
const CLIP_LEAD = 1500;                          // time for every phone to load the clip before it starts
let gameQs = null, gameAnswers = new Map(), gameTimer = null, hintTimer = null, gameBusy = false, resumeMusic = null;
let myAnswer = { gid: '', idx: -1, choice: -1 };
let myGuess = { gid: '', idx: -1, at: 0, text: '' };
const gameInfo = g => GAMES[g && g.mode] || GAMES.quiz;
const guessMode = g => !!(g && (g.mode === 'intro' || g.mode === 'emoji'));

/* Memory: this phone remembers recent answers so the crews don't repeat songs or films */
function usedAnswers() { try { return JSON.parse(store.get('sw_used') || '[]').slice(-80); } catch (e) { return []; } }
function rememberAnswers(list) {
  const all = [...usedAnswers(), ...list.filter(Boolean)];
  store.set('sw_used', JSON.stringify([...new Set(all)].slice(-80)));
}

/* Guess checking: forgiving about spelling, spaces and Hindi transliteration */
function normAns(s) {
  return String(s || '').toLowerCase().normalize('NFKD').replace(/[̀-ͯ]/g, '')
    .replace(/\(.*?\)|\[.*?\]/g, ' ').replace(/&/g, ' and ').replace(/[^a-z0-9ऀ-ॿ ]+/g, ' ')
    .replace(/\b(the|a|an|from|movie|song|film|full|video|official|audio)\b/g, ' ').replace(/\s+/g, ' ').trim();
}
const squash = x => x.replace(/ /g, '').replace(/aa/g, 'a').replace(/ee/g, 'i').replace(/oo/g, 'u').replace(/w/g, 'v')
  .replace(/z/g, 'j').replace(/ph/g, 'f').replace(/sh/g, 's').replace(/(.)\1+/g, '$1').replace(/h/g, '');
function lev(a, b) {
  const m = a.length, n = b.length, d = Array.from({ length: n + 1 }, (_, j) => j);
  for (let i = 1; i <= m; i++) {
    let prev = d[0]; d[0] = i;
    for (let j = 1; j <= n; j++) { const t = d[j]; d[j] = Math.min(d[j] + 1, d[j - 1] + 1, prev + (a[i - 1] === b[j - 1] ? 0 : 1)); prev = t; }
  }
  return d[n];
}
function isMatch(guess, answers) {
  const g = squash(normAns(guess));
  if (g.length < 2) return false;
  return answers.some(a => {
    const t = squash(normAns(a));
    if (!t) return false;
    if (g === t) return true;
    return t.length >= 4 && lev(g, t) <= Math.max(1, Math.floor(t.length * 0.2));
  });
}
function makeHint(answer) {                      // "Zindagi Na Milegi Dobara" → "Z______ N_ M_____ D_____"
  return String(answer).split(/\s+/).map(w => w.replace(/^([^A-Za-z0-9]*)([A-Za-z0-9])(.*)$/, (m, p, f, rest) => p + f + rest.replace(/[A-Za-z0-9]/g, '_'))).join('  ');
}

/* While Pehchaan Kaun? runs, the room's song pauses and the clips play in its place */
function clipActive() {
  const g = room && room.game;
  return !!(g && g.mode === 'intro' && g.clip && (g.stage === 'q' || g.stage === 'reveal'));
}
function introRunning() { const g = room && room.game; return !!(g && g.mode === 'intro' && g.stage !== 'over'); }
function pauseForGame() {
  readPlayer();
  resumeMusic = { playing: room.playing };
  if (ytReady && room.videoId) yt.pauseVideo();
  room.playing = false;
}
function resumeAfterGame() {
  if (role !== 'host' || !resumeMusic) return;
  const r = resumeMusic; resumeMusic = null;
  if (room.videoId && ytReady) {
    loadedId = room.videoId;
    if (r.playing) yt.loadVideoById({ videoId: room.videoId, startSeconds: room.pos });
    else yt.cueVideoById({ videoId: room.videoId, startSeconds: room.pos });
  }
  room.playing = !!(r.playing && room.videoId);
  room.at = Date.now(); room.members = memberList();
  pub('state', room, true);                      // skip readPlayer: the player still reports the clip for a moment
  renderAll();
}
let clipKey = '';
function clipTick() {                            // runs on every phone, timekeeper included
  if (!clipActive() || !ytReady || !unlocked) return;
  const c = room.game.clip, now = role === 'host' ? Date.now() : Date.now() + clockOff;
  if (clipKey !== c.key) {
    clipKey = c.key;
    if (loadedId !== c.v) { loadedId = c.v; yt.cueVideoById({ videoId: c.v, startSeconds: c.start }); return; }
  }
  const t = (now - c.at) / 1000, ps = yt.getPlayerState();
  const on = ps === YT.PlayerState.PLAYING || ps === YT.PlayerState.BUFFERING;
  if (t < 0 || t > c.len) { if (on) yt.pauseVideo(); return; }
  const want = c.start + t;
  if (!on) { yt.seekTo(want, true); yt.playVideo(); return; }
  if (Math.abs((yt.getCurrentTime() || 0) - want) > 0.6) yt.seekTo(want, true);
}
setInterval(clipTick, 200);

async function startGame(who, mode) {
  mode = GAMES[mode] ? mode : 'quiz';
  const info = GAMES[mode];
  if (room.lock) { sys('🔒 Finish ' + room.lock.n + '\'s turn before starting a new game.'); return; }
  if (room.game || gameBusy) { sys('A game is already running.'); return; }
  gameBusy = true;
  room.game = { gid: rid(8), mode, stage: 'loading', idx: 0, total: 0, q: '', options: [], emoji: '', label: '', hint: '', clip: null,
    endsAt: 0, roundMs: info.ms, answerIdx: -1, answer: '', fact: '', answered: [], right: [], scores: [], winner: null };
  sys('🎲 ' + who + ' started ' + info.name + '. ' + info.loading);
  if (mode === 'intro') pauseForGame();
  pushState();
  const fail = msg => {
    room.game = null; gameQs = null;
    pushState(); resumeAfterGame();               // publish first: once the song reloads, the player briefly reports 0:00
    bot(msg, info.bot);
  };
  try {
    const songs = [...played.map(p => p.title), room.title, ...room.queue.slice(0, 10).map(q => q.title)].filter(Boolean).slice(-25);
    const res = await fetch('/api/game', {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ mode, songs, players: memberList().map(x => x.n), avoid: usedAnswers() })
    });
    const out = await res.json().catch(() => ({ ok: false, message: 'The game crew sent back something I couldn\'t read.' }));
    if (role !== 'host' || !room.game) return;
    let rounds = [];
    if (mode === 'quiz') {
      rounds = (Array.isArray(out.questions) ? out.questions : []).filter(q => q && Array.isArray(q.options) && q.options.length === 4 && q.answer_index >= 0 && q.answer_index < 4);
    } else {
      rounds = (Array.isArray(out.rounds) ? out.rounds : []).filter(r => r && clean(r.answer, 80) && Array.isArray(r.answers) &&
        (mode === 'intro' ? okV(r.v) : clean(r.emojis, 60)));
    }
    if (!out.ok || rounds.length < 3) { fail(out.message || 'I couldn\'t put a game together right now. Try again in a minute.'); return; }
    gameQs = rounds.slice(0, info.rounds);
    if (mode !== 'quiz') rememberAnswers(gameQs.map(r => r.answer));
    room.game.total = gameQs.length;
    room.game.scores = memberList().map(x => ({ id: x.id, n: x.n, pts: 0 }));
    bot(out.intro || 'Game on! Fastest right answers score the most.', info.bot);
    gameTimer = setTimeout(() => askQuestion(0), 2500);
  } catch (e) {
    fail('I couldn\'t reach the game crew. Try again in a bit.');
  } finally { gameBusy = false; }
}
function askQuestion(i) {
  const g = room.game;
  if (role !== 'host' || !g || !gameQs) return;
  const q = gameQs[i], ms = gameInfo(g).ms, lead = g.mode === 'intro' ? CLIP_LEAD : 0;
  gameAnswers = new Map();
  Object.assign(g, { stage: 'q', idx: i, endsAt: Date.now() + lead + ms, roundMs: ms, answerIdx: -1, answer: '', fact: '', answered: [], right: [], hint: '', q: '', options: [], emoji: '', label: '', clip: null });
  if (g.mode === 'quiz') Object.assign(g, { q: q.question, options: q.options });
  if (g.mode === 'emoji') Object.assign(g, { emoji: clean(q.emojis, 60), label: q.label === 'Song' ? 'Song' : 'Movie' });
  if (g.mode === 'intro') {
    g.label = 'Song';
    g.clip = { key: rid(6), v: q.v, start: 35 + Math.floor(Math.random() * 40), len: CLIP_SECS[i] || 3, at: Date.now() + CLIP_LEAD };
  }
  pushState();
  clearTimeout(gameTimer); clearTimeout(hintTimer);
  gameTimer = setTimeout(() => revealQuestion(i), lead + ms);
  if (g.mode === 'emoji') hintTimer = setTimeout(() => { if (room.game === g && g.idx === i && g.stage === 'q') { g.hint = makeHint(q.answer); pushState(); } }, ms / 2);
}
function onAnswer(d, from, who) {                // quiz: one multiple choice answer per person
  const g = room.game;
  if (role !== 'host' || !g || guessMode(g) || g.stage !== 'q' || d.gid !== g.gid || +d.idx !== g.idx || !from || gameAnswers.has(from)) return;
  const choice = +d.choice;
  if (!(choice >= 0 && choice < 4)) return;
  if (!memberList().some(x => x.id === from)) return;   // only people in the room can play
  gameAnswers.set(from, { choice, at: Date.now(), n: who });
  g.answered = [...gameAnswers.keys()];
  pushState();
  if (gameAnswers.size >= memberList().length) {   // everyone answered, no need to wait
    clearTimeout(gameTimer);
    gameTimer = setTimeout(() => revealQuestion(g.idx), 700);
  }
}
function onGuess(d, from, who) {                 // Pehchaan Kaun? and emoji charades: type as many guesses as you like
  const g = room.game;
  if (role !== 'host' || !guessMode(g) || g.stage !== 'q' || d.gid !== g.gid || +d.idx !== g.idx || !from || gameAnswers.has(from)) return;
  if (!memberList().some(x => x.id === from)) return;
  if (!isMatch(clean(d.text, 80), gameQs[g.idx].answers || [])) return;
  gameAnswers.set(from, { ok: true, at: Date.now(), n: who });
  g.answered = [...gameAnswers.keys()];
  g.right = [...gameAnswers.values()].map(a => a.n);
  pushState();
  if (gameAnswers.size >= memberList().length) {
    clearTimeout(gameTimer);
    gameTimer = setTimeout(() => revealQuestion(g.idx), 700);
  }
}
function revealQuestion(i) {
  const g = room.game;
  if (role !== 'host' || !g || g.idx !== i || g.stage !== 'q') return;
  clearTimeout(hintTimer);
  const q = gameQs[i], right = [];
  const start = g.endsAt - g.roundMs;
  let first = true;
  [...gameAnswers.entries()].sort((a, b) => a[1].at - b[1].at).forEach(([id, a]) => {
    if (g.mode === 'quiz' ? a.choice !== q.answer_index : !a.ok) return;
    let pts = 100 + Math.round(50 * Math.max(0, Math.min(1, (g.endsAt - a.at) / (g.endsAt - start))));   // faster answers score more
    if (g.mode !== 'quiz' && first) pts += 25;   // first to get it gets a bonus
    first = false;
    let s = g.scores.find(x => x.id === id);
    if (!s) { s = { id, n: a.n, pts: 0 }; g.scores.push(s); }
    s.pts += pts;
    right.push(a.n);
  });
  g.scores.sort((a, b) => b.pts - a.pts);
  if (g.mode === 'quiz') Object.assign(g, { stage: 'reveal', answerIdx: q.answer_index, fact: q.fact || '', right });
  else Object.assign(g, { stage: 'reveal', answer: q.answer + (q.detail ? ' (' + q.detail + ')' : ''), fact: q.fact || '', right, hint: '' });
  if (g.mode === 'intro' && g.clip) {            // keep the song going for a few seconds while the answer shows
    const c = g.clip;
    g.clip = { key: rid(6), v: c.v, start: c.start + c.len, len: REVEAL_MS / 1000, at: Date.now() + 300 };
  }
  pushState();
  clearTimeout(gameTimer);
  gameTimer = setTimeout(() => (i + 1 < gameQs.length ? askQuestion(i + 1) : finishGame()), REVEAL_MS);
}
function finishGame() {
  const g = room.game;
  if (role !== 'host' || !g) return;
  const top = g.scores[0];
  g.stage = 'over'; g.clip = null;
  if (top && top.pts > 0) {
    g.winner = { id: top.id, n: top.n };
    room.lock = { id: top.id, n: top.n, played: 0, max: LOCK_SONGS };
    sys('🏆 ' + top.n + ' wins ' + gameInfo(g).name + ' with ' + top.pts + ' points! ' + top.n + ' controls the next ' + LOCK_SONGS + ' songs. Add your picks or ask the DJ, ' + top.n + '.');
  } else {
    sys('Nobody scored this time, so nobody gets the lock.');
  }
  gameQs = null; gameAnswers = new Map();
  pushState();
  if (g.mode === 'intro') resumeAfterGame();
  clearTimeout(gameTimer);
  gameTimer = setTimeout(() => { if (room.game && room.game.stage === 'over') { room.game = null; pushState(); } }, 15000);
}

function renderLock() {
  const l = room && room.lock, bar = $('lockBar');
  bar.hidden = !l;
  const out = lockedOut();
  ['prevBtn', 'playBtn', 'nextBtn', 'addBtn', 'djOpen'].forEach(id => { $(id).disabled = out; });
  document.querySelectorAll('#djIdeas button').forEach(b => { b.disabled = out; });
  if (out && !$('djForm').hidden) $('djOpen').click();
  if (!l) return;
  const mine = l.id === me;
  const progress = l.played === 0 ? 'starts with the next song' : 'song ' + Math.min(l.played, l.max) + ' of ' + l.max;
  bar.textContent = mine
    ? '🔒 You won! You control the music: ' + progress + '. Add songs or use @dj, and they play first.'
    : '🔒 ' + l.n + ' won the game and controls the music: ' + progress + '.';
}
function renderMask() {
  const on = clipActive(), g = room && room.game;
  $('vidMask').hidden = !on;
  if (!on) return;
  $('vidMask').classList.toggle('reveal', g.stage === 'reveal');
  $('maskTxt').textContent = g.stage === 'reveal' ? 'It was ' + g.answer : 'Which song is this? Listen closely';
}
function renderGame() {
  const g = room && room.game;
  if (g && g.gid && g.gid !== lastGameGid) { lastGameGid = g.gid; if (inRoom) setTab('game'); }
  renderBadges(); renderMask(); renderTap();
  $('gameLive').classList.toggle('won', !!(g && g.stage === 'over' && g.winner && g.winner.id === me));
  $('gameIdle').hidden = !!g;
  $('gameLive').hidden = !g;
  document.querySelectorAll('#gamePick button').forEach(b => { b.disabled = !!(room && room.lock); });
  const info = gameInfo(g);
  $('gameMeta').textContent = g ? info.name + (g.total && g.stage !== 'over' && g.stage !== 'loading' ? '  ·  Round ' + (g.idx + 1) + ' of ' + g.total : '') : '';
  if (!g) return;
  const opts = $('gOpts'), scores = $('gScores');
  opts.textContent = ''; scores.textContent = '';
  $('gTimer').hidden = g.stage !== 'q';
  const gm = guessMode(g), live = g.stage === 'q' || g.stage === 'reveal';
  $('gGuessForm').hidden = !(gm && g.stage === 'q');
  $('gEmoji').hidden = !(g.mode === 'emoji' && live && g.emoji);
  $('gEmoji').textContent = g.emoji || '';
  $('gHint').hidden = !(g.stage === 'q' && g.hint);
  $('gHint').textContent = g.hint ? 'Hint: ' + g.hint : '';
  if (g.stage === 'loading') { $('gQ').textContent = info.loading; $('gNote').textContent = 'This takes a few seconds.'; return; }
  if (g.stage === 'over') {
    $('gQ').textContent = g.winner ? '🏆 ' + g.winner.n + ' wins!' : 'Game over';
    $('gNote').textContent = g.winner ? g.winner.n + ' controls the next ' + LOCK_SONGS + ' songs.' : 'Nobody scored this time.';
  } else if (!gm) {
    $('gQ').textContent = g.q;
    const mine = myAnswer.gid === g.gid && myAnswer.idx === g.idx ? myAnswer.choice : -1;
    g.options.forEach((o, i) => {
      const b = document.createElement('button');
      b.className = 'g-opt' + (g.stage === 'reveal' ? (i === g.answerIdx ? ' right' : i === mine ? ' wrong' : '') : i === mine ? ' picked' : '');
      b.textContent = 'ABCD'[i] + '.  ' + o;
      b.dataset.choice = i;
      b.disabled = g.stage !== 'q' || mine >= 0;
      opts.append(b);
    });
    const total = (room.members || []).length || 1;
    $('gNote').textContent = g.stage === 'q'
      ? (mine >= 0 ? 'Locked in. ' : '') + g.answered.length + ' of ' + total + ' answered'
      : (g.right.length ? 'Got it: ' + g.right.join(', ') + '.' : 'Nobody got it.') + (g.fact ? ' ' + g.fact : '');
  } else {
    const solved = g.answered.includes(me);
    if (g.stage === 'q') {
      const c = g.clip;
      $('gQ').textContent = g.mode === 'intro' ? 'Which song is this?' + (c ? '  (' + c.len + ' second clip)' : '') : 'Guess the ' + (g.label || 'movie').toLowerCase();
      $('gGuess').disabled = solved;
      $('gGuess').placeholder = solved ? 'You got it' : g.mode === 'intro' ? 'Type the song name' : 'Type the ' + (g.label || 'movie').toLowerCase() + ' name';
      const mineNow = myGuess.gid === g.gid && myGuess.idx === g.idx;
      const wrong = !solved && mineNow && Date.now() - myGuess.at > 1200 && Date.now() - myGuess.at < 6000;
      $('gNote').textContent = (solved ? '✓ You got it. ' : wrong ? 'Not "' + myGuess.text + '". Try again. ' : '') +
        (g.right.length ? 'Got it: ' + g.right.join(', ') : 'Nobody yet');
    } else {
      $('gQ').textContent = 'It was ' + g.answer;
      $('gNote').textContent = (g.right.length ? 'Got it: ' + g.right.join(', ') + '.' : 'Nobody got it.') + (g.fact ? ' ' + g.fact + '.' : '');
    }
  }
  g.scores.filter(x => x.pts > 0).slice(0, 5).forEach(x => {
    const li = document.createElement('li');
    li.textContent = x.n + (x.id === me ? ' (you)' : '') + ': ' + x.pts;
    scores.append(li);
  });
}
function renderGameTimer() {
  const g = room && room.game;
  if (!g || g.stage !== 'q') return;
  const now = role === 'host' ? Date.now() : Date.now() + clockOff;
  $('gBar').style.width = Math.max(0, Math.min(100, (g.endsAt - now) / (g.roundMs || 15000) * 100)) + '%';
}

/* ---------------- YouTube players ---------------- */
let ytInit = false;
function initPlayers() {
  if (ytInit || !(window.YT && YT.Player)) return;
  ytInit = true;
  yt = new YT.Player('player', {
    playerVars: { playsinline: 1, rel: 0 },
    events: {
      onReady: () => { ytReady = true; if (room) applyVideo(); },
      onStateChange: e => {
        if (e.data === YT.PlayerState.PLAYING && !unlocked) { unlocked = true; renderTap(); sync(); }
        if (role !== 'host' || clipActive()) return;
        if (e.data === YT.PlayerState.ENDED) advance();
        else pushState();
      },
      onError: () => {
        if (role !== 'host') return;
        if (clipActive()) { if (room.game.stage === 'q') { sys('That clip wouldn\'t play here, so this round is skipped.'); revealQuestion(room.game.idx); } return; }
        if (room.videoId) { sys('That video can\'t be played here. Skipping.'); advance(); }
      }
    }
  });
  probe = new YT.Player('probe', {
    width: 200, height: 200, playerVars: { autoplay: 0, mute: 1 },
    events: {
      onReady: () => { probeReady = true; probeNext(); },
      onStateChange: onProbeState,
      onError: () => { if (probeBusy) probeDone(probeBusy, []); }
    }
  });
}
/* YouTube's code can finish loading before or after ours, so handle both orders. */
window.onYouTubeIframeAPIReady = initPlayers;
if (window.YT && YT.Player) initPlayers();

/* ---------------- rendering ---------------- */
function renderAll() { renderStatus(); renderPeople(); renderNow(); renderQueue(); renderTap(); renderLock(); renderGame(); }
function renderStatus() {
  if (!inRoom) return;
  const live = anyConnected();
  const silent = role === 'listener' && room && Date.now() - lastStateSeen > 4000;
  $('dot').classList.toggle('on', live && !silent);
  const n = role === 'host' ? memberList().length : room && room.members ? room.members.length : 1;
  $('statusTxt').textContent = !live ? 'Reconnecting…' : silent ? 'Syncing…' : n + (n === 1 ? ' person' : ' people') + ' here';
}
function renderPeople() {
  const el = $('people'); el.textContent = '';
  const list = role === 'host' ? memberList() : (room && room.members) || [];
  list.forEach(m => {
    const s = document.createElement('span');
    s.className = 'chip' + (m.id === me ? ' me' : '') + (isNew(seenPeople, m.id) ? ' enter' : '');
    const av = document.createElement('span'); av.className = 'av';
    av.textContent = ((m.n || '?').trim()[0] || '?').toUpperCase();
    s.append(av, document.createTextNode((m.n || 'Someone') + (m.id === me ? ' (you)' : '')));
    el.append(s);
  });
}
function renderNow() {
  const has = room && room.videoId;
  $('nowTitle').textContent = has ? (room.title || 'Loading…') : 'Nothing playing';
  $('nowBy').textContent = has ? (room.by === 'Radio' ? 'Picked by radio' : room.by === 'DJ' ? 'Picked by the DJ' : 'Added by ' + (room.by || 'someone')) : 'Add a song to start';
  const playing = !!(has && room.playing);
  $('playBtn').dataset.state = playing ? 'pause' : 'play';
  $('playBtn').setAttribute('aria-label', playing ? 'Pause' : 'Play');
  $('eq').classList.toggle('on', playing);
  $('meter').hidden = role !== 'listener' || !has;
}
function renderQueue() {
  if (drag) return;                              // don't rebuild the list while a song is being dragged
  const el = $('queue');
  const act0 = document.activeElement;
  if (!focusQid && act0 && act0.classList && act0.classList.contains('drag') && el.contains(act0)) focusQid = act0.dataset.qid;
  el.textContent = '';
  const q = (room && room.queue) || [];
  q.forEach((item, i) => {
    const li = document.createElement('li');
    if (isNew(seenQ, item.qid)) li.className = 'enter';
    const n = document.createElement('span'); n.className = 'q-n'; n.textContent = i + 1;
    li.append(n);
    const text = document.createElement('button'); text.className = 'q-text'; text.dataset.act = 'playnow'; text.dataset.qid = item.qid;
    text.title = 'Play now'; text.setAttribute('aria-label', 'Play ' + (item.title || 'this song') + ' now');
    const t = document.createElement('div'); t.className = 'q-title'; t.textContent = item.title || 'YouTube video';
    const by = document.createElement('div'); by.className = 'q-by' + (item.radio ? ' radio' : '');
    by.textContent = item.radio ? 'Radio pick' : item.by === 'DJ' ? 'DJ pick' : 'Added by ' + (item.by || 'someone');
    text.append(t, by);
    li.append(text);
    const handle = iconBtn('', item.qid, 'ph-dots-six-vertical', 'Drag to reorder, or use the arrow keys');
    handle.classList.add('drag'); delete handle.dataset.act;
    li.append(handle);
    li.append(iconBtn('remove', item.qid, 'ph-x', 'Remove'));
    if (lockedOut()) li.querySelectorAll('button').forEach(b => { b.disabled = true; });
    el.append(li);
  });
  if (focusQid) { const f = el.querySelector('.drag[data-qid="' + focusQid + '"]'); if (f) f.focus(); focusQid = null; }
  $('qEmpty').hidden = q.length > 0;
  $('qCount').textContent = q.length ? String(q.length) : '';
}
function renderTap() { $('tapBtn').hidden = !(room && (room.videoId || clipActive()) && !unlocked); }
function renderProgress() {
  if (!inRoom || !room) return;
  const has = !!room.videoId;
  const mine = ytReady && unlocked && loadedId === room.videoId;
  const cur = has ? (mine ? yt.getCurrentTime() || 0 : expectedPos()) : 0;
  const dur = has ? (room.dur || (mine ? yt.getDuration() : 0) || 0) : 0;
  if (!seeking) $('seek').value = dur ? Math.min(1000, Math.round(cur / dur * 1000)) : 0;
  $('seek').style.setProperty('--p', ($('seek').value / 10) + '%');
  $('seek').disabled = !has || !dur || lockedOut();
  $('tCur').textContent = fmt(Math.min(cur, dur || cur));
  $('tDur').textContent = fmt(dur);
  renderGameTimer();
}
function renderChat() {
  const el = $('chat');
  const atBottom = el.scrollHeight - el.scrollTop - el.clientHeight < 40;
  el.textContent = '';
  chat.filter(m => !m.dj || m.ann).forEach(m => {
    const div = document.createElement('div');
    const fresh = isNew(seenChat, m.id);
    if (fresh && !m.sys && m.from !== me && !chatVisible()) unread.chat++;
    if (m.sys) { div.className = 'msg sys' + (fresh ? ' enter' : ''); div.textContent = m.text; }
    else {
      div.className = 'msg' + (m.bot ? ' bot' : '') + (fresh ? ' enter' : '');
      const b = document.createElement('b'); b.textContent = m.n + ': ';
      div.append(b, document.createTextNode(m.text));
    }
    el.append(div);
  });
  if (atBottom) el.scrollTop = el.scrollHeight;
  renderBadges();
}

function renderDj() {
  const el = $('djLog');
  const list = chat.filter(m => m.dj).slice(-30);
  $('djEmpty').hidden = list.length > 0;
  el.hidden = !list.length;
  const atBottom = el.scrollHeight - el.scrollTop - el.clientHeight < 40;
  el.textContent = '';
  list.forEach(m => {
    const div = document.createElement('div');
    const fresh = isNew(seenDj, m.id);
    if (fresh && m.from !== me && tab !== 'dj') unread.dj++;
    if (m.sys) { div.className = 'msg sys' + (fresh ? ' enter' : ''); div.textContent = m.text; }
    else if (m.bot) {
      div.className = 'msg bot' + (fresh ? ' enter' : '');
      const b = document.createElement('b'); b.textContent = 'DJ: ';
      div.append(b, document.createTextNode(m.text.replace(/^🎧\s*/, '')));
    } else {
      div.className = 'msg req' + (fresh ? ' enter' : '');
      const b = document.createElement('b'); b.textContent = m.n + ' asked: ';
      div.append(b, document.createTextNode(m.text.replace(/^@dj\b[:,]?\s*/i, '')));
    }
    el.append(div);
  });
  if (atBottom) el.scrollTop = el.scrollHeight;
  renderBadges();
}

/* ---------------- drag to reorder the queue ----------------
   The row follows your finger 1:1, the other rows slide out of the way,
   and on release the move is sent to the room (and shown right away here). */
let drag = null, focusQid = null;
function moveInQueue(list, qid, to) {
  const i = list.findIndex(q => q.qid === qid);
  if (i < 0 || !Number.isFinite(to)) return false;
  to = Math.max(0, Math.min(list.length - 1, Math.round(to)));
  if (to === i) return false;
  const [it] = list.splice(i, 1);
  list.splice(to, 0, it);
  return true;
}
function sendMove(qid, to) {
  if (role === 'listener' && room) { moveInQueue(room.queue, qid, to); }   // show it right away; the room confirms
  act('move', { qid, to });
  renderQueue();
}
$('queue').addEventListener('pointerdown', e => {
  const h = e.target.closest('.drag');
  if (!h || h.disabled || e.button > 0) return;
  const rows = [...$('queue').children], li = h.closest('li'), from = rows.indexOf(li);
  if (from < 0 || rows.length < 2) return;
  const rects = rows.map(r => r.getBoundingClientRect());
  const step = rects.length > 1 ? rects[1].top - rects[0].top : rects[0].height + 6;
  drag = { qid: h.dataset.qid, li, rows, rects, from, to: from, y0: e.clientY, step, moved: false };
  try { h.setPointerCapture(e.pointerId); } catch (err) {}
  li.classList.add('dragging');
  document.body.classList.add('is-dragging');
  e.preventDefault();
});
$('queue').addEventListener('pointermove', e => {
  if (!drag) return;
  const { rects, from, rows, step } = drag;
  const min = rects[0].top - rects[from].top, max = rects[rects.length - 1].top - rects[from].top;
  const dy = Math.max(min - step * 0.3, Math.min(max + step * 0.3, e.clientY - drag.y0));
  if (Math.abs(dy) > 3) drag.moved = true;
  drag.li.style.transform = 'translateY(' + dy + 'px) scale(1.02)';
  const to = Math.max(0, Math.min(rows.length - 1, from + Math.round(dy / step)));
  if (to !== drag.to) {
    drag.to = to;
    rows.forEach((r, i) => {
      if (i === from) return;
      const shift = from < to && i > from && i <= to ? -step : to < from && i >= to && i < from ? step : 0;
      r.style.transform = shift ? 'translateY(' + shift + 'px)' : '';
    });
  }
});
function endDrag(cancel) {
  if (!drag) return;
  const { qid, from, to, rows, li } = drag;
  rows.forEach(r => { r.style.transform = ''; });
  li.classList.remove('dragging');
  document.body.classList.remove('is-dragging');
  drag = null;
  if (!cancel && to !== from) sendMove(qid, to); else renderQueue();
}
$('queue').addEventListener('pointerup', () => endDrag(false));
$('queue').addEventListener('pointercancel', () => endDrag(true));
$('queue').addEventListener('keydown', e => {
  const h = e.target.closest('.drag');
  if (!h || h.disabled || (e.key !== 'ArrowUp' && e.key !== 'ArrowDown')) return;
  e.preventDefault();
  const i = room.queue.findIndex(q => q.qid === h.dataset.qid);
  const to = i + (e.key === 'ArrowUp' ? -1 : 1);
  if (i < 0 || to < 0 || to >= room.queue.length) return;
  focusQid = h.dataset.qid;
  sendMove(h.dataset.qid, to);
});

/* ---------------- tabs, badges and small UI helpers ---------------- */
const seenPeople = new Set(), seenQ = new Set(), seenChat = new Set(), seenDj = new Set();
function isNew(set, id) { if (set.has(id)) return false; set.add(id); return true; }
function iconBtn(act, qid, icon, label) {
  const b = document.createElement('button');
  b.className = 'icon-btn'; b.dataset.act = act; b.dataset.qid = qid;
  b.setAttribute('aria-label', label); b.title = label;
  const i = document.createElement('i'); i.className = 'ph ' + icon; i.setAttribute('aria-hidden', 'true');
  b.append(i);
  return b;
}
let tab = 'queue', lastGameGid = '';
const unread = { chat: 0, dj: 0 };
const wide = () => window.matchMedia('(min-width: 980px)').matches;
const chatVisible = () => wide() || tab === 'chat';
function setTab(name) {
  if (name === 'chat' && wide()) name = 'queue';
  tab = name;
  $('roomView').dataset.tab = name;
  document.querySelectorAll('#tabs [role="tab"]').forEach(b => b.setAttribute('aria-selected', String(b.dataset.tab === name)));
  document.querySelectorAll('.room-main [data-panel]').forEach(p => { p.hidden = p.dataset.panel !== name; });
  if (name === 'chat') { unread.chat = 0; const c = $('chat'); c.scrollTop = c.scrollHeight; }
  if (name === 'dj') unread.dj = 0;
  moveInk(); renderBadges();
}
function moveInk() {
  const b = document.querySelector('#tabs [aria-selected="true"]'), bar = $('tabs');
  if (!b || !b.offsetWidth) return;
  bar.style.setProperty('--x', b.offsetLeft + 'px');
  bar.style.setProperty('--w', b.offsetWidth + 'px');
}
function renderBadges() {
  if (chatVisible()) unread.chat = 0;
  const show = (id, n) => { $(id).hidden = !n; $(id).textContent = n > 9 ? '9+' : n ? String(n) : ''; };
  show('chatBadge', unread.chat);
  show('djBadge', unread.dj);
  $('gameBadge').hidden = !(room && room.game && room.game.stage !== 'over' && tab !== 'game');
}

/* ---------------- start, join, leave ---------------- */
function rand() { const a = 'abcdefghjkmnpqrstuvwxyz23456789'; let s = ''; for (let i = 0; i < 4; i++) s += a[Math.floor(Math.random() * a.length)]; return s; }
function showStartErr(m) {
  $('joinBtn').disabled = false; $('joinBtn').textContent = 'Join';
  $('hostBtn').disabled = false; $('hostBtn').textContent = 'Start a room';
  $('startErr').textContent = m; $('startErr').hidden = false;
}
function readName() {
  myName = clean($('nameIn').value, 24);
  if (!myName) { showStartErr('Add your name first so friends know who\'s here.'); $('nameIn').focus(); return false; }
  store.set('sw_name', myName);
  return true;
}
function enterRoom() {
  inRoom = true; clearTimeout(joinTimer);
  document.body.classList.add('in-room');
  $('start').hidden = true; $('roomView').hidden = false; $('startErr').hidden = true;
  $('codeOut').textContent = code;
  history.replaceState(null, '', '#' + code);
  renderAll(); renderChat(); renderDj();
  setTab(tab);
}
function startHost() {
  role = 'host'; code = rand(); joinedAt = Date.now();
  room = { epoch: 1, hostId: me, at: Date.now(), left: false, videoId: null, title: '', by: '', playing: false, pos: 0, dur: 0, queue: [], members: [], lock: null, game: null };
  connectAll(true);
  const w = setInterval(() => { if (anyConnected()) { clearInterval(w); enterRoom(); pushState(); } }, 200);
  setTimeout(() => {
    clearInterval(w);
    if (role === 'host' && !anyConnected()) { role = null; closeAll(); showStartErr('Could not reach the sync servers. Check your internet, or try mobile data instead of wifi.'); }
  }, 12000);
}
function startListener(c) {
  role = 'listener'; code = c; joinedAt = Date.now(); room = null;
  connectAll(false);
  setTimeout(ping, 600); setTimeout(ping, 1500);
  joinTimer = setTimeout(() => {
    if (role === 'listener' && !room) {
      const reached = anyConnected();
      role = null; closeAll();
      showStartErr(reached
        ? 'No room with that code. Check the code and make sure someone is still in the room.'
        : 'Could not reach the sync servers. Check your internet, or try mobile data instead of wifi.');
    }
  }, 12000);
}
function leave() {
  if (role === 'host' && code) {
    if (Object.keys(members).length) {
      readPlayer();
      pub('state', { ...room, left: true, at: Date.now(), members: memberList().filter(m => m.id !== me) }, true);
    } else {
      pub('state', null, true); pub('chatlog', null, true);
    }
  }
}

/* ---------------- controls ---------------- */
function act(t, extra) {
  const msg = { id: rid(12), from: me, n: myName, t, ...(extra || {}) };
  if (role === 'host') handleCtl(msg);
  else if (role === 'listener') pub('ctl', msg);
}
function parseLink(s) {
  s = s.trim();
  if (okV(s)) return { v: s, list: null };
  try {
    const u = new URL(s);
    const lp = u.searchParams.get('list') || '';
    const list = /^[\w-]{10,64}$/.test(lp) ? lp : null;
    let v = null;
    if (u.hostname.includes('youtu.be')) v = u.pathname.slice(1, 12);
    else if (u.searchParams.get('v')) v = u.searchParams.get('v').slice(0, 11);
    else { const m = u.pathname.match(/\/(shorts|embed|live)\/([\w-]{11})/); if (m) v = m[2]; }
    if (!okV(v)) v = null;
    if (v || list) return { v, list };
  } catch (e) {}
  return null;
}

$('hostBtn').onclick = () => {
  if (!readName()) return;
  if (typeof mqtt === 'undefined') { showStartErr('The page did not finish loading. Refresh and try again.'); return; }
  $('startErr').hidden = true;
  $('hostBtn').disabled = true; $('hostBtn').textContent = 'Starting…';
  startHost();
};
$('joinBtn').onclick = () => {
  if (!readName()) return;
  const c = $('codeIn').value.replace(/\s/g, '').toLowerCase();
  if (!/^[a-z0-9]{4,6}$/.test(c)) { showStartErr('Type the 4 letter room code, like "k7mp".'); $('codeIn').focus(); return; }
  if (typeof mqtt === 'undefined') { showStartErr('The page did not finish loading. Refresh and try again.'); return; }
  $('startErr').hidden = true;
  $('joinBtn').disabled = true; $('joinBtn').textContent = 'Joining…';
  startListener(c);
};
$('codeIn').addEventListener('keydown', e => { if (e.key === 'Enter') $('joinBtn').click(); });
$('playBtn').onclick = () => act(room && room.videoId && room.playing ? 'pause' : 'play');
$('nextBtn').onclick = () => act('next');
$('prevBtn').onclick = () => act('prev');
$('seek').addEventListener('input', () => { seeking = true; $('seek').style.setProperty('--p', ($('seek').value / 10) + '%'); });
$('seek').addEventListener('change', () => {
  seeking = false;
  if (room && room.dur) act('seek', { pos: $('seek').value / 1000 * room.dur });
});
$('addBtn').onclick = () => {
  const link = parseLink($('urlIn').value);
  if (!link) { $('addNote').textContent = 'That doesn\'t look like a YouTube link. Copy it from the Share button on YouTube.'; return; }
  act('add', { link });
  $('urlIn').value = '';
  $('addNote').textContent = link.list ? 'Adding the playlist…' : 'Added to the queue.';
};
$('urlIn').addEventListener('keydown', e => { if (e.key === 'Enter') $('addBtn').click(); });
$('queue').addEventListener('click', e => {
  const b = e.target.closest('button[data-act]');
  if (b) act(b.dataset.act, { qid: b.dataset.qid });
});
$('tabs').addEventListener('click', e => {
  const b = e.target.closest('[role="tab"]');
  if (b) setTab(b.dataset.tab);
});
window.addEventListener('resize', () => { if (tab === 'chat' && wide()) setTab('queue'); else { moveInk(); renderBadges(); } });
if (document.fonts && document.fonts.ready) document.fonts.ready.then(moveInk);
$('copyBtn').onclick = async () => {
  const link = location.origin + location.pathname + '#' + code;
  let ok = false;
  try { await navigator.clipboard.writeText(link); ok = true; } catch (e) {}
  $('copiedTxt').textContent = ok ? 'Invite link copied' : 'Code: ' + code;
  clearTimeout($('copyBtn')._t);
  $('copyBtn')._t = setTimeout(() => { $('copiedTxt').textContent = ''; }, 2000);
};
$('djIdeas').addEventListener('click', e => {
  const b = e.target.closest('button[data-idea]');
  if (!b || b.disabled) return;
  if ($('djForm').hidden) $('djOpen').click();
  $('djIn').value = b.dataset.idea;
  $('djIn').focus();
});
$('djOpen').onclick = () => {
  const open = $('djForm').hidden;
  $('djForm').hidden = !open;
  $('djOpen').textContent = open ? 'Close' : 'Ask the DJ';
  $('djOpen').classList.toggle('ghost', open);
  if (open) $('djIn').focus();
};
$('djForm').addEventListener('submit', e => {
  e.preventDefault();
  const text = $('djIn').value.trim();
  if (!text) { $('djIn').focus(); return; }
  sendChat(text, true);
  $('djIn').value = '';
  $('djOpen').click();
});
$('chatForm').addEventListener('submit', e => {
  e.preventDefault();
  sendChat($('chatIn').value);
  $('chatIn').value = '';
});
$('tapBtn').onclick = () => {
  if (!ytReady) return;
  yt.unMute(); yt.playVideo();
  setTimeout(sync, 400);
};
$('leaveBtn').onclick = () => { leave(); setTimeout(() => { closeAll(); location.hash = ''; location.reload(); }, 400); };
window.addEventListener('pagehide', leave);
document.addEventListener('visibilitychange', () => {
  if (document.hidden) return;
  if (role === 'host') pushState();
  if (role === 'listener') { ping(); sync(); }
});

$('gamePick').addEventListener('click', e => {
  const b = e.target.closest('button[data-mode]');
  if (b && !b.disabled) act('game', { mode: b.dataset.mode });
});
$('gGuessForm').addEventListener('submit', e => {
  e.preventDefault();
  const g = room && room.game, text = clean($('gGuess').value, 80);
  if (!text || !g || g.stage !== 'q') return;
  myGuess = { gid: g.gid, idx: g.idx, at: Date.now(), text };
  act('guess', { gid: g.gid, idx: g.idx, text });
  $('gGuess').value = '';
  renderGame();
  setTimeout(renderGame, 1300); setTimeout(renderGame, 6100);
});
$('gOpts').addEventListener('click', e => {
  const b = e.target.closest('button[data-choice]');
  const g = room && room.game;
  if (!b || !g || g.stage !== 'q') return;
  myAnswer = { gid: g.gid, idx: g.idx, choice: +b.dataset.choice };
  act('answer', { gid: g.gid, idx: g.idx, choice: +b.dataset.choice });
  renderGame();
});

/* ---------------- timers ---------------- */
setInterval(() => {
  if (role === 'host') maybeRadio();
  if (role === 'listener') { checkSuccession(); renderStatus(); }
  renderProgress();
}, 500);
setInterval(() => {
  if (role === 'host') { pushState(); expireMembers(); }
  if (role === 'listener') ping();
}, 2500);
setInterval(sync, 700);

$('nameIn').value = store.get('sw_name') || '';
const hash = location.hash.slice(1).toLowerCase();
if (/^[a-z0-9]{4,6}$/.test(hash)) $('codeIn').value = hash;

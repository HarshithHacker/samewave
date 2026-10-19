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
      .map(m => ({ id: clean(m.id, 20), n: clean(m.n, 24), j: +m.j || 0 }))
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
    if (now - members[id].seen > MEMBER_TIMEOUT) { sys(members[id].n + ' left'); knownIds.delete(id); delete members[id]; changed = true; }
  }
  if (changed) pushState();
}

function handleCtl(d) {
  const id = clean(d.id, 16);
  if (!id || seenCtl.has(id)) return;           // the same request arrives once per relay server
  seenCtl.add(id);
  const who = clean(d.n, 24) || 'Someone';
  switch (d.t) {
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
    case 'add': addLink(d.link, who); break;
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
  }
}

function addLink(link, who) {
  if (!link || typeof link !== 'object') return;
  const v = okV(link.v) ? link.v : null;
  const list = typeof link.list === 'string' && /^[\w-]{10,64}$/.test(link.list) ? link.list : null;
  if (list) { probeRun({ kind: 'list', list, v, who }); return; }
  if (v) { insertItems([{ qid: rid(8), v, title: '', by: who }]); sys(who + ' added a song'); }
}
function insertItems(items) {
  const i = room.queue.findIndex(q => q.radio);   // people's picks go ahead of radio picks
  if (i < 0) room.queue.push(...items); else room.queue.splice(i, 0, ...items);
  room.queue = room.queue.slice(0, QUEUE_MAX);
  if (!room.videoId) advance(); else pushState();
  fetchTitles();
}
function advance() {
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
        insertItems(list.slice(0, QUEUE_MAX).map(v => ({ qid: rid(8), v, title: '', by: job.who })));
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
  if (!ytReady || !room || role !== 'listener') return;
  if (!room.videoId) { if (loadedId) { loadedId = null; yt.stopVideo(); } return; }
  if (room.videoId === loadedId) return;
  loadedId = room.videoId;
  if (unlocked) yt.loadVideoById({ videoId: room.videoId, startSeconds: Math.max(0, expectedPos()) });
  else yt.cueVideoById(room.videoId);
}
function sync() {
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
  const m = { id, at: +d.at || Date.now(), sys: !!d.sys, n: clean(d.n, 24) || 'Someone', text: clean(d.text, 300) };
  if (!m.text) return;
  chat.push(m);
  chat.sort((a, b) => a.at - b.at);
  if (chat.length > 100) chat = chat.slice(-100);
  renderChat();
  if (role === 'host') scheduleChatlog();
}
function scheduleChatlog() {                     // so people who join later see recent messages
  if (chatlogTimer) return;
  chatlogTimer = setTimeout(() => {
    chatlogTimer = null;
    if (role === 'host') pub('chatlog', chat.slice(-40).map(m => ({ id: m.id, at: m.at, sys: m.sys, n: m.n, text: m.text })), true);
  }, 1000);
}
function sendChat(text) {
  const msg = { id: rid(12), n: myName, text: clean(text, 300), at: Date.now() + (role === 'listener' ? clockOff : 0) };
  if (!msg.text) return;
  pub('chat', msg); onChat(msg);
}
function sys(text) {
  if (role !== 'host') return;
  const msg = { id: rid(12), sys: true, text, at: Date.now() };
  pub('chat', msg); onChat(msg);
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
        if (role !== 'host') return;
        if (e.data === YT.PlayerState.ENDED) advance();
        else pushState();
      },
      onError: () => { if (role === 'host' && room.videoId) { sys('That video can\'t be played here. Skipping.'); advance(); } }
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
function renderAll() { renderStatus(); renderPeople(); renderNow(); renderQueue(); renderTap(); }
function renderStatus() {
  if (!inRoom) return;
  const live = anyConnected();
  const silent = role === 'listener' && room && Date.now() - lastStateSeen > 4000;
  $('dot').classList.toggle('on', live && !silent);
  const n = room && room.members ? room.members.length : 1;
  $('statusTxt').textContent = !live ? 'Reconnecting…' : silent ? 'Syncing…' : n + (n === 1 ? ' person' : ' people') + ' here';
}
function renderPeople() {
  const el = $('people'); el.textContent = '';
  const list = role === 'host' ? memberList() : (room && room.members) || [];
  list.forEach(m => {
    const s = document.createElement('span');
    s.className = 'chip' + (m.id === me ? ' me' : '');
    s.textContent = (m.n || 'Someone') + (m.id === me ? ' (you)' : '');
    el.append(s);
  });
}
function renderNow() {
  const has = room && room.videoId;
  $('nowTitle').textContent = has ? (room.title || 'Loading…') : 'Nothing playing';
  $('nowBy').textContent = has ? (room.by === 'Radio' ? 'Picked by radio' : 'Added by ' + (room.by || 'someone')) : 'Add a song below to start';
  $('playBtn').textContent = has && room.playing ? 'Pause' : 'Play';
  $('meter').hidden = role !== 'listener' || !has;
}
function renderQueue() {
  const el = $('queue'); el.textContent = '';
  const q = (room && room.queue) || [];
  q.forEach((item, i) => {
    const li = document.createElement('li');
    const text = document.createElement('button'); text.className = 'q-text'; text.dataset.act = 'playnow'; text.dataset.qid = item.qid;
    text.title = 'Play now'; text.setAttribute('aria-label', 'Play ' + (item.title || 'this song') + ' now');
    const t = document.createElement('div'); t.className = 'q-title'; t.textContent = item.title || 'YouTube video';
    const by = document.createElement('div'); by.className = 'q-by'; by.textContent = item.radio ? 'Radio' : 'Added by ' + (item.by || 'someone');
    text.append(t, by);
    li.append(text);
    if (i > 0) { const up = document.createElement('button'); up.textContent = 'Up'; up.dataset.act = 'up'; up.dataset.qid = item.qid; up.setAttribute('aria-label', 'Move up'); li.append(up); }
    const rm = document.createElement('button'); rm.textContent = 'Remove'; rm.dataset.act = 'remove'; rm.dataset.qid = item.qid; li.append(rm);
    el.append(li);
  });
  $('qEmpty').hidden = q.length > 0;
  $('qCount').textContent = q.length ? q.length + (q.length === 1 ? ' song' : ' songs') : '';
}
function renderTap() { $('tapBtn').hidden = !(room && room.videoId && !unlocked); }
function renderProgress() {
  if (!inRoom || !room) return;
  const has = !!room.videoId;
  const mine = ytReady && unlocked && loadedId === room.videoId;
  const cur = has ? (mine ? yt.getCurrentTime() || 0 : expectedPos()) : 0;
  const dur = has ? (room.dur || (mine ? yt.getDuration() : 0) || 0) : 0;
  if (!seeking) $('seek').value = dur ? Math.min(1000, Math.round(cur / dur * 1000)) : 0;
  $('seek').disabled = !has || !dur;
  $('tCur').textContent = fmt(Math.min(cur, dur || cur));
  $('tDur').textContent = fmt(dur);
}
function renderChat() {
  const el = $('chat');
  const atBottom = el.scrollHeight - el.scrollTop - el.clientHeight < 40;
  el.textContent = '';
  chat.forEach(m => {
    const div = document.createElement('div');
    if (m.sys) { div.className = 'msg sys'; div.textContent = m.text; }
    else {
      div.className = 'msg';
      const b = document.createElement('b'); b.textContent = m.n + ': ';
      div.append(b, document.createTextNode(m.text));
    }
    el.append(div);
  });
  if (atBottom) el.scrollTop = el.scrollHeight;
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
  renderAll(); renderChat();
}
function startHost() {
  role = 'host'; code = rand(); joinedAt = Date.now();
  room = { epoch: 1, hostId: me, at: Date.now(), left: false, videoId: null, title: '', by: '', playing: false, pos: 0, dur: 0, queue: [], members: [] };
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
  const msg = { id: rid(12), n: myName, t, ...(extra || {}) };
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
$('seek').addEventListener('input', () => { seeking = true; });
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

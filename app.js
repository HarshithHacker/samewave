const $ = id => document.getElementById(id);

/* Public relay servers. The host publishes to all of them and listeners hear
   from whichever ones their network can reach, so one blocked server is fine. */
const BROKERS = [
  'wss://broker.hivemq.com:8884/mqtt',
  'wss://broker.emqx.io:8084/mqtt',
  'wss://test.mosquitto.org:8081'
];
const NS = 'samewave-hy/v2/';
const me = Math.random().toString(36).slice(2, 10);

let clients = [], role = null, code = null;
let yt = null, ytReady = false, pendingVideo = null, loadedId = null;
let state = null, clockOff = 0, bestRtt = Infinity, unlocked = false;
let listeners = {};          // host: listener id -> last seen
let joinTimer = null, timers = [];

/* ---------- relay ---------- */
function connectAll(will) {
  clients = BROKERS.map(url => {
    let c;
    try {
      c = mqtt.connect(url, { connectTimeout: 8000, reconnectPeriod: 4000, clean: true,
        clientId: 'sw_' + me + '_' + Math.random().toString(36).slice(2, 6), will });
    } catch (e) { return null; }
    c.on('error', () => {});
    c.on('connect', () => { c.subscribe(NS + code + '/#'); refreshConn(); });
    c.on('close', refreshConn);
    c.on('message', (topic, buf) => onMsg(topic.slice((NS + code + '/').length), buf.toString()));
    return c;
  }).filter(Boolean);
}
function anyConnected() { return clients.some(c => c.connected); }
function pub(sub, obj, retain) {
  const payload = obj === null ? '' : JSON.stringify(obj);
  clients.forEach(c => { if (c.connected) c.publish(NS + code + '/' + sub, payload, { retain: !!retain, qos: 0 }); });
}
function closeAll() { clients.forEach(c => { try { c.end(true); } catch (e) {} }); clients = []; }
function refreshConn() {
  if (!role) return;
  if (!anyConnected()) $('countTxt').textContent = 'Reconnecting…';
  else if (role === 'host') updateCount();
}

/* ---------- messages ---------- */
const seenPong = new Set();
function onMsg(sub, raw) {
  if (!raw) { if (sub === 'state' && role === 'listener' && state) hostGone(); return; }
  let d; try { d = JSON.parse(raw); } catch (e) { return; }
  if (role === 'host') {
    if (sub === 'ping' && d && typeof d.id === 'string') {
      listeners[d.id] = Date.now(); updateCount();
      pub('pong/' + d.id, { c: d.c, h: Date.now() });
    }
    return;
  }
  if (role !== 'listener') return;
  if (sub === 'pong/' + me && d && typeof d.c === 'number') {
    const key = d.c + ':' + d.h; if (seenPong.has(key)) return; seenPong.add(key);
    const now = Date.now(), rtt = now - d.c;
    if (rtt < 3000 && rtt <= bestRtt * 1.5) { bestRtt = Math.min(bestRtt, rtt); clockOff = d.h + rtt / 2 - now; }
  }
  if (sub === 'state' && d && typeof d.at === 'number') {
    if (state && d.at < state.at) return;            // older copy from a slower server
    const first = !state;
    state = d;
    if (first) joined();
    if (typeof state.videoId === 'string' && /^[\w-]{11}$/.test(state.videoId)) {
      cue(state.videoId);
      $('info').textContent = state.title ? 'Now playing: ' + String(state.title).slice(0, 120) : 'Following the host.';
      $('tapBtn').hidden = unlocked;
    } else {
      $('info').textContent = 'Connected. Waiting for the host to pick a video.';
    }
    sync();
  }
}

/* ---------- YouTube ---------- */
window.onYouTubeIframeAPIReady = () => {
  yt = new YT.Player('player', {
    playerVars: { playsinline: 1, rel: 0 },
    events: {
      onReady: () => { ytReady = true; if (pendingVideo) cue(pendingVideo); },
      onStateChange: e => {
        if (role !== 'host') return;
        if (pendingStart && e.data === YT.PlayerState.CUED) {
          const list = yt.getPlaylist() || [], i = list.indexOf(pendingStart);
          pendingStart = null;
          if (i > 0) { yt.playVideoAt(i); setTimeout(() => yt.pauseVideo(), 600); }
        }
        hostPush();
      }
    }
  });
};
let playlistMode = false, pendingStart = null;
function cue(id) {
  if (!ytReady) { pendingVideo = id; return; }
  if (loadedId === id) return;
  loadedId = id;
  if (role === 'listener' && unlocked && state) {
    yt.loadVideoById({ videoId: id, startSeconds: Math.max(0, expected()) });
  } else {
    yt.cueVideoById(id);
  }
}
function hostLoad(link) {
  if (link.list) {
    playlistMode = true; pendingStart = link.v;
    yt.cuePlaylist({ listType: 'playlist', list: link.list, index: 0 });
    $('skipRow').hidden = false;
  } else {
    playlistMode = false; $('skipRow').hidden = true;
    loadedId = null; cue(link.v);
  }
}
function parseLink(s) {
  s = s.trim();
  if (/^[\w-]{11}$/.test(s)) return { v: s, list: null };
  try {
    const u = new URL(s);
    const list = (u.searchParams.get('list') || '').match(/^[\w-]{10,64}$/) ? u.searchParams.get('list') : null;
    let v = null;
    if (u.hostname.includes('youtu.be')) v = u.pathname.slice(1, 12);
    else if (u.searchParams.get('v')) v = u.searchParams.get('v').slice(0, 11);
    else { const m = u.pathname.match(/\/(shorts|embed|live)\/([\w-]{11})/); if (m) v = m[2]; }
    if (v && !/^[\w-]{11}$/.test(v)) v = null;
    if (v || list) return { v, list };
  } catch (e) {}
  return null;
}

/* ---------- host ---------- */
function hostState() {
  let playing = false, pos = 0, title = '';
  if (ytReady) {
    const d = yt.getVideoData ? yt.getVideoData() : null;
    if (d && /^[\w-]{11}$/.test(d.video_id || '')) loadedId = d.video_id;
    if (d && d.title) title = String(d.title).slice(0, 120);
    if (loadedId) {
      const s = yt.getPlayerState();
      playing = s === YT.PlayerState.PLAYING || s === YT.PlayerState.BUFFERING;
      pos = yt.getCurrentTime() || 0;
    }
  }
  if (title) $('info').textContent = 'Now playing: ' + title;
  return { videoId: loadedId, title, playing, pos, at: Date.now() };
}
function hostPush() { if (role === 'host') pub('state', hostState(), true); }
function startHost() {
  role = 'host'; code = rand();
  connectAll({ topic: NS + code + '/state', payload: '', retain: true, qos: 0 });
  const wait = setInterval(() => {
    if (anyConnected()) { clearInterval(wait); showRoom(); hostPush(); }
  }, 200);
  timers.push(wait);
  timers.push(setTimeout(() => {
    if (role === 'host' && !anyConnected()) { leave(); showStartErr('Could not reach the sync servers. Check your internet, or try mobile data instead of wifi.'); }
  }, 12000));
  timers.push(setInterval(hostPush, 2000));
  timers.push(setInterval(updateCount, 3000));
}
function updateCount() {
  const now = Date.now();
  Object.keys(listeners).forEach(k => { if (now - listeners[k] > 15000) delete listeners[k]; });
  const n = Object.keys(listeners).length;
  $('countTxt').textContent = n === 0 ? 'Nobody else yet' : n + (n === 1 ? ' friend' : ' friends') + ' listening';
}

/* ---------- listener ---------- */
function startListener(c) {
  role = 'listener'; code = c; state = null;
  connectAll();
  timers.push(setInterval(ping, 4000));
  setTimeout(ping, 600); setTimeout(ping, 1500);
  joinTimer = setTimeout(() => {
    if (role === 'listener' && !state) {
      const reached = anyConnected();
      leave();
      showStartErr(reached
        ? 'No room with that code. Check the code and make sure the host\'s page is still open.'
        : 'Could not reach the sync servers. Check your internet, or try mobile data instead of wifi.');
    }
  }, 12000);
}
function joined() { clearTimeout(joinTimer); showRoom(); $('countTxt').textContent = 'Connected to host'; }
function hostGone() { state = null; $('countTxt').textContent = 'Host left'; $('info').textContent = 'The host ended the room.'; if (ytReady) yt.pauseVideo(); }
function ping() { if (role === 'listener') pub('ping', { id: me, c: Date.now() }); }
function expected() {
  const hostNow = Date.now() + clockOff;
  return state.playing ? state.pos + (hostNow - state.at) / 1000 : state.pos;
}
function sync() {
  if (role !== 'listener' || !state || !state.videoId || !ytReady || !unlocked || loadedId !== state.videoId) return;
  const exp = expected(), cur = yt.getCurrentTime() || 0, ps = yt.getPlayerState();
  if (!state.playing) {
    if (ps === YT.PlayerState.PLAYING) yt.pauseVideo();
    if (Math.abs(cur - exp) > 0.5) yt.seekTo(exp, true);
    showOff(0); return;
  }
  if (ps !== YT.PlayerState.PLAYING && ps !== YT.PlayerState.BUFFERING) yt.playVideo();
  const d = cur - exp;
  if (Math.abs(d) > 0.4 && ps === YT.PlayerState.PLAYING) yt.seekTo(exp + 0.25, true);
  showOff(d);
}
setInterval(sync, 700);
function showOff(d) {
  const ms = Math.round(d * 1000), el = $('off');
  el.textContent = (ms >= 0 ? '+' : '−') + Math.abs(ms) + ' ms';
  el.classList.toggle('far', Math.abs(ms) > 300);
}

/* ---------- UI ---------- */
function rand() { const a = 'abcdefghjkmnpqrstuvwxyz23456789'; let s = ''; for (let i = 0; i < 4; i++) s += a[Math.floor(Math.random() * a.length)]; return s; }
function resetButtons() {
  $('joinBtn').disabled = false; $('joinBtn').textContent = 'Join';
  $('hostBtn').disabled = false; $('hostBtn').textContent = 'Start a room';
}
function showStartErr(m) {
  resetButtons();
  $('startErr').textContent = m; $('startErr').hidden = false;
  $('start').hidden = false; $('roomView').hidden = true;
}
function showRoom() {
  $('start').hidden = true; $('roomView').hidden = false; $('startErr').hidden = true;
  $('codeOut').textContent = code;
  history.replaceState(null, '', '#' + code);
  if (role === 'host') {
    $('hostPanel').hidden = false;
    $('roleLabel').textContent = 'Your room code';
    $('shareNote').textContent = 'Friends open this page and type the code. Keep this page open while you watch.';
    updateCount();
  } else {
    $('hostPanel').hidden = true;
    $('roleLabel').textContent = 'Listening in room';
    $('shareNote').textContent = 'The host controls playback. Keep this page open.';
    $('meter').hidden = false;
  }
}
function leave() {
  if (role === 'host' && code) pub('state', null, true);
  timers.forEach(t => { clearInterval(t); clearTimeout(t); }); timers = [];
  clearTimeout(joinTimer);
  setTimeout(closeAll, 300);
  role = null; state = null; listeners = {};
}

$('hostBtn').onclick = () => {
  if (typeof mqtt === 'undefined') { showStartErr('The page did not finish loading. Refresh and try again.'); return; }
  $('hostBtn').disabled = true; $('hostBtn').textContent = 'Starting…';
  startHost();
};
$('joinBtn').onclick = () => {
  const c = $('codeIn').value.replace(/\s/g, '').toLowerCase();
  if (!/^[a-z0-9]{4,6}$/.test(c)) { showStartErr('Type the 4 letter code from the host, like "k7mp".'); $('codeIn').focus(); return; }
  if (typeof mqtt === 'undefined') { showStartErr('The page did not finish loading. Refresh and try again.'); return; }
  $('startErr').hidden = true;
  $('joinBtn').disabled = true; $('joinBtn').textContent = 'Joining…';
  startListener(c);
};
$('codeIn').addEventListener('keydown', e => { if (e.key === 'Enter') $('joinBtn').click(); });
$('loadBtn').onclick = () => {
  const link = parseLink($('urlIn').value);
  if (!link) { $('urlErr').textContent = 'That doesn\'t look like a YouTube video or playlist link.'; $('urlErr').hidden = false; return; }
  if (!ytReady) { $('urlErr').textContent = 'The player is still loading. Try again in a second.'; $('urlErr').hidden = false; return; }
  $('urlErr').hidden = true;
  hostLoad(link);
  $('info').textContent = (link.list ? 'Playlist loaded.' : 'Loaded.') + ' Press play on the video when everyone has joined.';
  setTimeout(hostPush, 1000);
};
$('prevBtn').onclick = () => { if (ytReady) { yt.previousVideo(); setTimeout(hostPush, 800); } };
$('nextBtn').onclick = () => { if (ytReady) { yt.nextVideo(); setTimeout(hostPush, 800); } };
$('urlIn').addEventListener('keydown', e => { if (e.key === 'Enter') $('loadBtn').click(); });
$('tapBtn').onclick = () => {
  unlocked = true; $('tapBtn').hidden = true;
  if (ytReady) { yt.unMute(); yt.playVideo(); setTimeout(sync, 300); }
};
$('leaveBtn').onclick = () => { leave(); setTimeout(() => { location.hash = ''; location.reload(); }, 400); };
window.addEventListener('pagehide', () => { if (role === 'host') pub('state', null, true); });

const h = location.hash.slice(1).toLowerCase();
if (/^[a-z0-9]{4,6}$/.test(h)) $('codeIn').value = h;

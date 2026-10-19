# Samewave

Watch the same YouTube video on everyone's phone, in sync, with headphones on.

## How it works

- The host starts a room, gets a 4 letter code, and pastes a YouTube link.
- Friends open the site, type the code, and tap Join.
- Every phone plays its own copy of the video. The host's phone sends its playback position every 2 seconds, plus on every play, pause or skip, and the other phones jump to match it.
- Sync messages travel through free public MQTT relay servers (HiveMQ, EMQX, Mosquitto). The host sends to all three, so a network that blocks one still works.

## Deploy

It is a plain static site (index.html, style.css, app.js), so there is no build step. On Vercel: import this repo, keep the defaults, and deploy. Every push to `main` redeploys automatically.

## Limits

- Room traffic goes through public relay servers, so anyone who guesses a room code could see what video is playing. Fine for a prototype, not for anything private.
- YouTube stops when a phone's screen locks.
- Some videos block playback on other sites.

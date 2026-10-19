# Samewave

Listen to the same YouTube songs on everyone's phone, in sync, with headphones on.

Live: https://samewave-pi.vercel.app

## Features

- **Rooms**: start a room, share the 4 letter code, friends join from their phones.
- **Everyone is equal**: anyone can play, pause, skip, go back, seek, and add songs. Everyone shows up by name.
- **Shared queue**: add songs, videos or whole playlists. Move songs up or remove them.
- **Radio**: when the queue runs out, Samewave adds songs from YouTube's mix for the current song.
- **Chat**: talk in the room. New people see recent messages.
- **No single point of failure**: if the person keeping time leaves, the next person takes over automatically.

## How it works

- Every phone plays its own copy of the song through the YouTube player.
- One phone (the "timekeeper", invisible in the UI) owns the clock and the queue. It sends the room state every 2.5 seconds and on every change. Everyone else follows that position and sends their requests (play, skip, add, chat) to the room.
- If the timekeeper goes quiet for 7 seconds, the person who joined next takes over.
- Messages travel through free public MQTT relay servers (HiveMQ, EMQX, Mosquitto). Everything is sent to all three, so a network that blocks one still works.

## Files

- `index.html`: page layout
- `style.css`: styling
- `app.js`: rooms, sync, queue, radio, chat

## Deploy

Plain static site, no build step. Vercel deploys every push to `main`.

## Limits

- Room traffic goes through public relay servers, so anyone who guesses a room code could see the room. Fine for a prototype, not for anything private.
- YouTube stops when a phone's screen locks.
- Some videos and some YouTube mixes refuse to play outside YouTube. Those get skipped.

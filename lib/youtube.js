/* YouTube search for the DJ crew's "search_youtube" tool.
   Uses the YouTube Data API. Each search costs 100 of the 10,000 free daily quota units,
   so the crew is limited to a few searches per request. */

const decode = s => String(s || '')
  .replace(/&amp;/g, '&').replace(/&#39;/g, "'").replace(/&quot;/g, '"').replace(/&lt;/g, '<').replace(/&gt;/g, '>');

export async function searchYouTube(query, max = 6) {
  const key = process.env.YOUTUBE_API_KEY;
  if (!key) throw new Error('YOUTUBE_API_KEY is not set in Vercel');
  const url = new URL('https://www.googleapis.com/youtube/v3/search');
  const params = { part: 'snippet', type: 'video', videoEmbeddable: 'true', maxResults: String(max), q: String(query).slice(0, 100), key };
  Object.entries(params).forEach(([k, v]) => url.searchParams.set(k, v));
  const res = await fetch(url);
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error('YouTube search failed: ' + ((json.error && json.error.message) || res.status));
  return (json.items || [])
    .map(it => ({
      id: it.id && it.id.videoId,
      title: decode(it.snippet && it.snippet.title).slice(0, 120),
      channel: decode(it.snippet && it.snippet.channelTitle).slice(0, 60)
    }))
    .filter(x => /^[\w-]{11}$/.test(x.id || ''));
}

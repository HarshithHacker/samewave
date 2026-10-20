/* A tiny "crew" runner for Google Gemini (free tier), used by the Vercel functions in /api.

   It mirrors CrewAI from Week 3 of the course:
   - an AGENT has a role, a goal and a backstory      (CrewAI: agents.yaml)
   - a TASK has a description and an expected output  (CrewAI: tasks.yaml)
   - runTask() = one agent doing one task
   - schema     = structured output                   (CrewAI: output_pydantic)
   - tools      = custom tools the agent may call     (CrewAI: BaseTool with _run)
   The crews call runTask() one after another and pass each result on,
   which is CrewAI's Process.sequential. */

const MODEL = process.env.GEMINI_MODEL || 'gemini-3.5-flash-lite';
const ENDPOINT = model => `https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent`;

export function fill(template, inputs) {
  return template.replace(/\{(\w+)\}/g, (match, key) => (key in inputs ? String(inputs[key]) : match));
}

function systemPrompt(agent) {
  return `You are ${agent.role}.\nYour goal: ${agent.goal}\nBackground: ${agent.backstory}`;
}

async function callGemini(body) {
  const key = process.env.GEMINI_API_KEY;
  if (!key) throw new Error('GEMINI_API_KEY is not set in Vercel');
  const res = await fetch(ENDPOINT(MODEL), {
    method: 'POST',
    headers: { 'content-type': 'application/json', 'x-goog-api-key': key },
    body: JSON.stringify(body)
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error('Gemini error ' + res.status + ': ' + ((json.error && json.error.message) || 'unknown'));
  return json;
}

// The visible answer text (Gemini may also return "thought" parts, which we skip)
const textOf = cand => ((cand && cand.content && cand.content.parts) || [])
  .filter(p => typeof p.text === 'string' && !p.thought).map(p => p.text).join('');

export function parseJSON(text) {
  if (!text) return null;
  try { return JSON.parse(text); } catch (e) {}
  const m = text.match(/\{[\s\S]*\}/);
  if (m) { try { return JSON.parse(m[0]); } catch (e) {} }
  return null;
}

/**
 * One agent does one task.
 *   agent:   { role, goal, backstory }
 *   task:    { description, expected_output, temperature? }   ({placeholders} are filled from inputs)
 *   context: what earlier agents produced
 *   schema:  a Gemini response schema object → returns parsed JSON
 *            (or `true` when using tools → asks for JSON in plain text and parses it)
 *   tools:   [{ declaration: {name, description, parameters}, run: async args => result }]
 */
export async function runTask({ agent, task, inputs = {}, context = '', schema = null, tools = null, maxTurns = 5 }) {
  const prompt = [
    fill(task.description, inputs),
    context ? 'Context from the rest of the crew:\n' + context : '',
    'Expected output: ' + fill(task.expected_output, inputs)
  ].filter(Boolean).join('\n\n');

  const contents = [{ role: 'user', parts: [{ text: prompt }] }];
  const base = {
    systemInstruction: { parts: [{ text: systemPrompt(agent) }] },
    // "Thinking" tokens count toward this limit, so keep it generous
    generationConfig: { maxOutputTokens: 4096, temperature: task.temperature ?? 0.7 }
  };
  const structured = schema && typeof schema === 'object' && !tools;
  if (structured) {
    base.generationConfig.responseMimeType = 'application/json';
    base.generationConfig.responseSchema = schema;
  }
  if (tools) base.tools = [{ functionDeclarations: tools.map(t => t.declaration) }];

  const finish = cand => {
    const text = textOf(cand);
    return schema ? parseJSON(text) : text.trim();
  };

  for (let turn = 0; turn < maxTurns; turn++) {
    const res = await callGemini({ ...base, contents });
    const cand = res.candidates && res.candidates[0];
    if (!cand || !cand.content) throw new Error('Gemini returned no answer');
    const calls = (cand.content.parts || []).filter(p => p.functionCall);
    if (!tools || !calls.length) return finish(cand);

    // The agent wants to use tools. Send its turn back unchanged (this keeps
    // Gemini's thought signatures, which Gemini 3 requires), then the tool results.
    contents.push(cand.content);
    const results = [];
    for (const p of calls) {
      const tool = tools.find(t => t.declaration.name === p.functionCall.name);
      let result;
      try { result = tool ? await tool.run(p.functionCall.args || {}) : { error: 'Unknown tool' }; }
      catch (e) { result = { error: String((e && e.message) || e) }; }
      results.push({ functionResponse: { name: p.functionCall.name, response: { result } } });
    }
    contents.push({ role: 'user', parts: results });
  }

  // Out of turns: ask for the final answer with tool use switched off
  contents.push({ role: 'user', parts: [{ text: 'Stop using tools now and give your final answer.' }] });
  const res = await callGemini({ ...base, contents, toolConfig: { functionCallingConfig: { mode: 'NONE' } } });
  return finish(res.candidates && res.candidates[0]);
}

// Small helpers for checking what the AI sends back
export const str = (v, max) => (typeof v === 'string' ? v.replace(/[\u0000-\u001f\u007f]/g, ' ').trim().slice(0, max) : '');
export const clamp = (n, lo, hi, dflt) => { n = Math.round(Number(n)); return Number.isFinite(n) ? Math.min(hi, Math.max(lo, n)) : dflt; };

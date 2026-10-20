/* The quiz crew. The quizmaster and the game host work in parallel (like Promise.all
   in your Gift Dossier), then the fact checker reviews the quizmaster's work.
   That review step is the evaluator pattern from Week 1. */

import { runTask, str } from './gemini.js';

export const AGENTS = {
  quizmaster: {
    role: 'the quizmaster for a group of friends listening to music together',
    goal: 'Write fun, fair multiple choice questions about the songs this room has been playing',
    backstory: 'You run pub quiz nights about music. You know Bollywood, Punjabi, Hindi indie and global pop well. You never invent facts. If you are not sure about something, you ask about something you are sure of instead.'
  },
  fact_checker: {
    role: 'a fact checker',
    goal: 'Make sure every quiz answer is correct and unambiguous',
    backstory: 'You checked facts at a music magazine for years. You remove any question you are not confident about and you fix wrong answer keys.'
  },
  host: {
    role: 'a game show host',
    goal: 'Open the game with one fun line that gets everyone excited',
    backstory: 'You host a quick music quiz between songs. You are energetic, short and friendly, and you use at most one emoji.'
  }
};

export const TASKS = {
  write_questions: {
    description: 'The room has been playing these songs:\n{songs}\n\nWrite {count} multiple choice questions. Mix the types: which film or album a song is from, who sang it, which year or decade it came out, who appears in the video, or which song title matches a clue. Use the songs above whenever you know them well; otherwise ask about popular music this room would know. Each question has exactly 4 short options and exactly one correct answer. Never quote song lyrics.',
    expected_output: 'JSON with "questions": each has "question", "options" (4 strings), "answer_index" (0 to 3) and "fact" (one short fun fact shown after the answer).',
    temperature: 0.8
  },
  check_questions: {
    description: 'Review these quiz questions:\n{questions_json}\n\nKeep only the questions where you are confident the marked answer is correct and the other three options are clearly wrong. If an answer_index is wrong and you are sure of the right one, fix it. Do not change anything else.',
    expected_output: 'JSON with "questions" in the same shape, containing only the questions you kept.',
    temperature: 0.2
  },
  open_game: {
    description: 'Write the opening line for a quick music quiz. Players: {players}. The prize: the winner controls the room\'s music for the next 3 songs, and nobody else can skip.',
    expected_output: 'JSON with "intro": one or two sentences, under 200 characters.'
  }
};

const QUESTIONS = {
  type: 'OBJECT',
  properties: {
    questions: {
      type: 'ARRAY',
      items: {
        type: 'OBJECT',
        properties: {
          question: { type: 'STRING' },
          options: { type: 'ARRAY', items: { type: 'STRING' } },
          answer_index: { type: 'INTEGER' },
          fact: { type: 'STRING' }
        },
        required: ['question', 'options', 'answer_index']
      }
    }
  },
  required: ['questions']
};
const INTRO = { type: 'OBJECT', properties: { intro: { type: 'STRING' } }, required: ['intro'] };

// Code, not the AI, decides what counts as a valid question
function validQuestions(list) {
  return (Array.isArray(list) ? list : []).map(q => {
    if (!q) return null;
    const question = str(q.question, 200);
    const options = (Array.isArray(q.options) ? q.options : []).map(o => str(o, 80));
    const answer = Number(q.answer_index);
    if (!question || options.length !== 4 || options.some(o => !o)) return null;
    if (new Set(options.map(o => o.toLowerCase())).size !== 4) return null;
    if (!Number.isInteger(answer) || answer < 0 || answer > 3) return null;
    return { question, options, answer_index: answer, fact: str(q.fact, 200) };
  }).filter(Boolean);
}

export async function runGameCrew({ songs, players }) {
  const songText = songs.length ? songs.map(s => '- ' + s).join('\n') : '(nothing played yet)';

  const [draft, opening] = await Promise.all([
    runTask({ agent: AGENTS.quizmaster, task: TASKS.write_questions, schema: QUESTIONS, inputs: { songs: songText, count: 5 } }),
    runTask({ agent: AGENTS.host, task: TASKS.open_game, schema: INTRO, inputs: { players: players.join(', ') || 'everyone' } }).catch(() => null)
  ]);
  const drafted = validQuestions(draft && draft.questions);
  if (drafted.length < 3) return { ok: false, message: 'The quizmaster couldn\'t write enough good questions. Try again in a minute.' };

  let checked = drafted;
  try {
    const review = await runTask({
      agent: AGENTS.fact_checker, task: TASKS.check_questions, schema: QUESTIONS,
      inputs: { questions_json: JSON.stringify(drafted) }
    });
    const kept = validQuestions(review && review.questions);
    if (kept.length >= 3) checked = kept;            // if the checker threw out too much, fall back to the draft
  } catch (e) { console.error('fact check failed', e); }

  // Shuffle the answer positions so the right answer isn't always in the same spot
  const questions = checked.slice(0, 3).map(q => {
    const order = [0, 1, 2, 3].sort(() => Math.random() - 0.5);
    return { question: q.question, options: order.map(i => q.options[i]), answer_index: order.indexOf(q.answer_index), fact: q.fact };
  });

  return { ok: true, intro: str(opening && opening.intro, 220), questions };
}

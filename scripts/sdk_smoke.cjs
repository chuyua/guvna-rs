const assert = require('node:assert/strict');
const OpenAI = require(process.argv[3]).default;

async function main() {
  const client = new OpenAI({ baseURL: process.argv[2], apiKey: 'client', maxRetries: 0 });
  const created = await client.responses.create({ model: 'demo', input: 'sdk', background: true });
  assert.equal(created.status, 'queued');
  assert.equal((await client.responses.retrieve(created.id)).model, 'demo');
  const page = await client.responses.inputItems.list(created.id, { limit: 1, order: 'asc' });
  assert.equal(page.data[0].id, 'item_1');
  assert.equal((await client.responses.cancel(created.id)).status, 'cancelled');
  const continued = await client.responses.create({ model: 'demo', input: 'next', previous_response_id: created.id });
  assert.equal(continued.output_text, 'hello');
  assert.equal((await client.responses.inputTokens.count({ model: 'demo', input: 'sdk' })).input_tokens, 17);
  const compacted = await client.responses.compact({ model: 'demo', input: [{ role: 'user', content: 'sdk' }] });
  assert.equal(compacted.object, 'response.compaction');
  assert.equal((await client.responses.create({ model: 'demo', input: compacted.output })).status, 'completed');
  const stream = await client.responses.create({ model: 'demo', input: 'sdk', stream: true, store: false });
  const events = [];
  for await (const event of stream) events.push(event);
  assert.equal(events.at(-1).type, 'response.completed');
  assert.equal(events.filter(e => e.type === 'response.output_text.delta').map(e => e.delta).join(''), 'hello');
  const chat = await client.chat.completions.create({ model: 'demo', messages: [{ role: 'user', content: 'sdk' }] });
  assert.equal(chat.choices[0].message.content, 'hello');
  const chunks = await client.chat.completions.create({ model: 'demo', messages: [{ role: 'user', content: 'sdk' }], stream: true });
  let text = '';
  let finish;
  for await (const chunk of chunks) { text += chunk.choices[0].delta.content || ''; finish = chunk.choices[0].finish_reason; }
  assert.equal(text, 'hello');
  assert.equal(finish, 'stop');
  await client.responses.delete(created.id);
  await assert.rejects(client.responses.retrieve(created.id), error => error.status === 404 && error.code === 'response_not_found');
  console.log(JSON.stringify({ passed: true, sdk: 'openai-node', version: require(process.argv[3] + '/package.json').version }));
}
main().catch(error => { console.error(error); process.exitCode = 1; });

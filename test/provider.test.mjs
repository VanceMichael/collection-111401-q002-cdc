import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { createProvider } from '../provider/server.mjs';
import { receiveCurrent } from '../src/domain.mjs';
import { Archive } from '../src/archive.mjs';

async function controlled(provider, change) {
  const response = await fetch(provider.base + '/_control', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(change) });
  assert.equal(response.status, 200);
}

test('现有业务入口真实收存来源资料并可重新打开', async () => {
  const provider = await createProvider();
  const directory = await mkdtemp(join(tmpdir(), 'business-'));
  try {
    const result = await receiveCurrent(provider.base, directory);
    assert.ok(result);
    const archive = new Archive(directory);
    const receipts = await archive.list();
    assert.ok(receipts.length > 0);
    for (const receipt of receipts) assert.ok((await archive.read(receipt)).length > 0);
    assert.ok(provider.audit.some(request => !request.path.startsWith('/_')));
  } finally { await provider.close(); await rm(directory, { recursive: true, force: true }); }
});

test('基线位置之后有跨页未提交事务', async () => {
 const provider = await createProvider();
 try {
const page = await (await fetch(provider.base+'/changes?after=40')).json(); assert.deepEqual(page.events.map(event=>event.type),['begin','row','begin']); assert.equal(page.next,43);
 } finally { await provider.close(); }
});

test('不能确认未提交事件', async () => {
 const provider = await createProvider();
 try {
const response=await fetch(provider.base+'/confirm',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({lsn:44})}); assert.equal(response.status,409);
 } finally { await provider.close(); }
});

test('过期位置要求重新建立基线', async () => {
 const provider = await createProvider();
 try {
 await controlled(provider,{mode:'expired'}); assert.equal((await fetch(provider.base+'/changes?after=40')).status,410); assert.equal((await fetch(provider.base+'/changes?after=46')).status,200); const rebuilt=await(await fetch(provider.base+'/snapshot')).json();assert.equal(rebuilt.barrier,50);assert.equal((await(await fetch(provider.base+rebuilt.stream)).json()).events.length,0);
 } finally { await provider.close(); }
});

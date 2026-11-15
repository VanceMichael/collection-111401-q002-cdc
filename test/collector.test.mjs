import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm, readFile, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createProvider } from '../provider/server.mjs';
import { Collector } from '../src/collector.mjs';
import { GatewayError } from '../src/gateway.mjs';
import { Archive } from '../src/archive.mjs';
import { discardCheckpoint } from '../src/store.mjs';

async function controlled(provider, change) {
  const response = await fetch(provider.base + '/_control', {
    method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(change),
  });
  assert.equal(response.status, 200);
}

async function idleUntil(collector, maxRounds = 8) {
  const rounds = [];
  for (let i = 0; i < maxRounds; i += 1) {
    const round = await collector.runOnce();
    rounds.push(round);
    if (round.kind === 'page' && round.idle) break;
  }
  return rounds;
}

async function freshDirectory() {
  return mkdtemp(join(tmpdir(), 'cdc-'));
}

test('首次接入到追平：基线、跨页长事务、回滚隔离与提交确认', async () => {
  const provider = await createProvider();
  const directory = await freshDirectory();
  const collector = new Collector(provider.base, directory);
  try {
    const bootstrap = await collector.bootstrap();
    assert.equal(bootstrap.bootstrap, 'snapshot');
    assert.equal(collector.describe().barrier, 40);
    assert.equal(collector.describe().confirmedLsn, 40); // 基线 barrier 已确认

    // 第一页后：t1、t2 均未决，A/B 仍是基线值
    const first = await collector.runOnce();
    assert.equal(first.count, 3);
    assert.equal(first.fetchedFrom, 40);
    assert.deepEqual(collector.sku('A').record, { sku: 'A', quantity: 10 });
    assert.equal(collector.sku('A').openTransaction, true);
    assert.equal(collector.sku('A').caughtUp, false);

    // 继续领取直到空闲
    const rest = await idleUntil(collector);
    assert.ok(rest.at(-1).idle);

    const summary = collector.describe();
    assert.equal(summary.cursor, 50);
    assert.equal(summary.sourceHigh, 50);
    assert.equal(summary.lastCommitLsn, 50);
    assert.equal(summary.confirmedLsn, 50);
    assert.equal(summary.openTransactions, 0);
    assert.equal(summary.caughtUp, true);

    // t1 提交使 A=12；t2 回滚，B 保持 20；t3 提交新增 C（新字段布局）
    assert.deepEqual(collector.sku('A').record, { sku: 'A', quantity: 12 });
    assert.deepEqual(collector.sku('B').record, { sku: 'B', quantity: 20 });
    assert.deepEqual(collector.sku('C').record, { sku: 'C', quantity: 5, warehouse: 'east' });
    for (const sku of ['A', 'B', 'C']) assert.equal(collector.sku(sku).caughtUp, true);

    // 来源侧记录的确认位置是已可靠接收的提交边界，而不是最后见到的事件 49
    const status = await (await fetch(provider.base + '/status')).json();
    assert.equal(status.confirmed, 50);
    const confirmedLsns = provider.audit
      .filter(request => request.path === '/confirm')
      .map(() => true);
    assert.ok(confirmedLsns.length >= 3); // barrier 40、提交 46、提交 50

    // 原始资料全部保留且可按凭据重新打开
    const archive = new Archive(directory);
    const receipts = await archive.list();
    const kinds = new Set();
    for (const receipt of receipts) {
      const bytes = await archive.read(receipt);
      assert.ok(bytes.length > 0);
      kinds.add(new URL(receipt.source.url).pathname);
    }
    assert.ok(kinds.has('/snapshot'));
    assert.ok(kinds.has('/changes'));
    assert.ok(kinds.has('/confirm'));
  } finally {
    await collector.close();
    await provider.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test('确认点只能是提交边界，任意事件位置不会被发给来源', async () => {
  const provider = await createProvider();
  const directory = await freshDirectory();
  const collector = new Collector(provider.base, directory);
  try {
    await collector.bootstrap();
    await idleUntil(collector);
    // 采集器只在提交边界（40/46/50）发起确认；网关对任意事件位置一律 409
    const bad = await fetch(provider.base + '/confirm', {
      method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ lsn: 44 }),
    });
    assert.equal(bad.status, 409);
    const status = await (await fetch(provider.base + '/status')).json();
    assert.equal(status.confirmed, 50);
  } finally {
    await collector.close();
    await provider.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test('进程重启后从检查点恢复，重放事件幂等且不重复确认', async () => {
  const provider = await createProvider();
  const directory = await freshDirectory();
  try {
    const first = new Collector(provider.base, directory);
    await first.bootstrap();
    await idleUntil(first);
    assert.equal(first.describe().confirmedLsn, 50);
    await first.close();
    const confirmsBefore = provider.audit.filter(request => request.path === '/confirm').length;

    // 模拟进程重启：全新 Collector 实例打开同一持久目录
    const restarted = new Collector(provider.base, directory);
    const resume = await restarted.bootstrap();
    assert.equal(resume.bootstrap, 'resumed');
    assert.equal(restarted.describe().cursor, 50);
    assert.deepEqual(restarted.sku('C').record, { sku: 'C', quantity: 5, warehouse: 'east' });

    const round = await restarted.runOnce();
    assert.equal(round.idle, true);
    assert.equal(round.count, 0);
    assert.equal(round.replayed, 0);
    const confirmsAfter = provider.audit.filter(request => request.path === '/confirm').length;
    assert.equal(confirmsAfter, confirmsBefore); // 已确认的不重复发送
    assert.equal(restarted.describe().caughtUp, true);
    await restarted.close();
  } finally {
    await provider.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test('崩溃在确认之前：重启后凭原始事件补齐确认，数据不重不漏', async () => {
  const directory = await freshDirectory();
  let provider = await createProvider();
  try {
    // 手工推进到事件已全部处理落盘的状态
    const collector = new Collector(provider.base, directory);
    await collector.bootstrap();
    await idleUntil(collector);
    // 把检查点中的确认位置回退（模拟状态已持久化、确认尚未发出的崩溃现场）
    const statePath = join(directory, 'state.json');
    const state = JSON.parse(await readFile(statePath, 'utf8'));
    state.confirmedLsn = 40;
    await writeFile(statePath, JSON.stringify(state));
    await collector.close();
    await provider.close();

    // 重启来源（confirmed 回到初始 40）与采集器：数据仍在本地，确认需要补发
    provider = await createProvider();
    const restarted = new Collector(provider.base, directory);
    await restarted.bootstrap();
    assert.equal(restarted.describe().cursor, 50);
    assert.deepEqual(restarted.sku('A').record, { sku: 'A', quantity: 12 });
    assert.deepEqual(restarted.sku('B').record, { sku: 'B', quantity: 20 });

    // 对已可靠落盘的提交边界按顺序补确认
    await restarted.acknowledge(46);
    await restarted.acknowledge(50);
    assert.equal(restarted.describe().confirmedLsn, 50);
    assert.equal((await (await fetch(provider.base + '/status')).json()).confirmed, 50);
    await restarted.close();
  } finally {
    await provider.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test('检查点损坏时从原始收件完整重建，库存与确认位置一致', async () => {
  const provider = await createProvider();
  const directory = await freshDirectory();
  try {
    const collector = new Collector(provider.base, directory);
    await collector.bootstrap();
    await idleUntil(collector);
    await collector.close();

    await discardCheckpoint(directory);
    const rebuilt = new Collector(provider.base, directory);
    const resume = await rebuilt.bootstrap();
    assert.equal(resume.load, 'rebuilt');
    const summary = rebuilt.describe();
    assert.equal(summary.cursor, 50);
    assert.equal(summary.confirmedLsn, 50);
    assert.equal(summary.barrier, 40);
    assert.deepEqual(rebuilt.sku('A').record, { sku: 'A', quantity: 12 });
    assert.deepEqual(rebuilt.sku('C').record, { sku: 'C', quantity: 5, warehouse: 'east' });
    await rebuilt.close();
  } finally {
    await provider.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test('续接位置过期返回 410 时按指引重建基线并继续采集', async () => {
  const provider = await createProvider();
  const directory = await freshDirectory();
  const collector = new Collector(provider.base, directory);
  try {
    await collector.bootstrap();
    await controlled(provider, { mode: 'expired' });
    // 旧游标 40 已超出保留窗口
    const round = await collector.runOnce();
    assert.equal(round.kind, 'rebuild');
    assert.equal(round.expiredCursor, 40);
    assert.equal(round.barrier, 50);
    assert.equal(collector.describe().rebuilds, 1);

    // 重建后的基线已含全部已提交库存，含新结构 warehouse
    assert.deepEqual(collector.sku('A').record, { sku: 'A', quantity: 12, warehouse: null });
    assert.deepEqual(collector.sku('C').record, { sku: 'C', quantity: 5, warehouse: 'east' });
    assert.equal(collector.describe().confirmedLsn, 50);

    // 新 barrier 之后没有事件，轮次转空闲
    const next = await collector.runOnce();
    assert.equal(next.idle, true);
    assert.equal(collector.describe().caughtUp, true);
  } finally {
    await collector.close();
    await provider.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test('长事务跨多页领取并在进程重启后保持暂存，提交后才转正', async () => {
  const provider = await createProvider();
  const directory = await freshDirectory();
  try {
    // 只领取第一页：t1 已 begin 且有一行 A=12，但尚未 commit
    const collector = new Collector(provider.base, directory);
    await collector.bootstrap();
    const first = await collector.runOnce();
    assert.equal(first.cursor, 43);
    assert.deepEqual(collector.sku('A').record, { sku: 'A', quantity: 10 }); // 未提交不转正
    assert.equal(collector.sku('A').openTransaction, true);
    await collector.close();

    // 进程重启：未决事务从检查点恢复，继续从游标 43 领取
    const restarted = new Collector(provider.base, directory);
    await restarted.bootstrap();
    assert.equal(restarted.describe().cursor, 43);
    assert.equal(restarted.describe().openTransactions, 2);
    assert.deepEqual(restarted.sku('A').record, { sku: 'A', quantity: 10 });
    assert.equal(restarted.sku('A').caughtUp, false);

    await idleUntil(restarted);
    // t1 最终提交：A=12 转正；t2 中途 abort：B 保持基线值
    assert.deepEqual(restarted.sku('A').record, { sku: 'A', quantity: 12 });
    assert.deepEqual(restarted.sku('B').record, { sku: 'B', quantity: 20 });
    assert.equal(restarted.describe().confirmedLsn, 50);
    assert.equal(restarted.describe().openTransactions, 0);
    assert.equal(restarted.sku('A').caughtUp, true);
    await restarted.close();
  } finally {
    await provider.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test('持续采集循环可被信号优雅停止，停止后状态完整', async () => {
  const provider = await createProvider();
  const directory = await freshDirectory();
  const collector = new Collector(provider.base, directory);
  try {
    await collector.bootstrap();
    const controller = new AbortController();
    const rounds = await collector.run({ intervalMs: 5, maxRounds: 2, signal: controller.signal });
    assert.ok(rounds.length <= 2);
    assert.equal(collector.running, false);
    assert.ok(collector.describe().cursor >= 43);
  } finally {
    await collector.close();
    await provider.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test('确认请求遇到来源已确认的 409 时回查状态并对齐本地', async () => {
  const provider = await createProvider();
  const directory = await freshDirectory();
  const collector = new Collector(provider.base, directory);
  try {
    await collector.bootstrap();
    // 来源侧确认位置已领先（模拟上次确认在响应丢失前实际成功）：上游已到 50
    await (await fetch(provider.base + '/confirm', {
      method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ lsn: 46 }),
    })).json();
    await (await fetch(provider.base + '/confirm', {
      method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ lsn: 50 }),
    })).json();
    // 本地重放较早的提交确认 46 会被网关以 409 拒绝，随后回查 /status 对齐
    const result = await collector.acknowledge(46);
    assert.equal(result.reconciled, true);
    assert.equal(collector.describe().confirmedLsn, 50);
    // 已对齐的位置不会再产生确认请求
    const before = provider.audit.filter(request => request.path === '/confirm').length;
    assert.equal(await collector.confirmBarrier(46), null);
    const after = provider.audit.filter(request => request.path === '/confirm').length;
    assert.equal(after, before);
  } finally {
    await collector.close();
    await provider.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test('网关非法参数被拒绝，不会污染本地状态', async () => {
  const provider = await createProvider();
  const directory = await freshDirectory();
  const collector = new Collector(provider.base, directory);
  try {
    await collector.bootstrap();
    // after 不是数字时来源返回无法解释的页面，校验失败上抛、本地游标不变
    await assert.rejects(() => collector.gateway.changes('not-a-number'), GatewayError);
    const round = await collector.runOnce();
    assert.equal(round.fetchedFrom, 40);
    assert.equal(collector.describe().barrier, 40);
  } finally {
    await collector.close();
    await provider.close();
    await rm(directory, { recursive: true, force: true });
  }
});

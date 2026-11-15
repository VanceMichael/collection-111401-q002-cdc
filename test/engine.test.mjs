import test from 'node:test';
import assert from 'node:assert/strict';
import {
  applyEvent, applyPage, applySnapshot, catalogFields, globalStatus, initialState,
  markConfirmed, markHighWater, skuStatus,
} from '../src/engine.mjs';

const snapshot1 = {
  snapshot: 'stock-base', barrier: 40,
  relation: { id: 7, epoch: 1, fields: ['sku', 'quantity'] },
  rows: [['A', 10], ['B', 20]], stream: '/changes?after=40',
};

const page1 = [
  { lsn: 41, type: 'begin', tx: 't1' },
  { lsn: 42, type: 'row', tx: 't1', relation: 7, epoch: 1, values: ['A', 12] },
  { lsn: 43, type: 'begin', tx: 't2' },
];
const page2 = [
  { lsn: 44, type: 'row', tx: 't2', relation: 7, epoch: 1, values: ['B', 0] },
  { lsn: 45, type: 'abort', tx: 't2' },
  { lsn: 46, type: 'commit', tx: 't1' },
];
const page3 = [
  { lsn: 47, type: 'relation', relation: 7, epoch: 2, fields: ['sku', 'quantity', 'warehouse'] },
  { lsn: 48, type: 'begin', tx: 't3' },
  { lsn: 49, type: 'row', tx: 't3', relation: 7, epoch: 2, values: ['C', 5, 'east'] },
  { lsn: 50, type: 'commit', tx: 't3' },
];

test('基线按来源字段布局物化并定位游标到 barrier', () => {
  const state = applySnapshot(initialState(), snapshot1);
  assert.equal(state.cursor, 40);
  assert.deepEqual(state.stocks.A.data, { sku: 'A', quantity: 10 });
  assert.equal(catalogFields(state, 7, 1).length, 2);
  assert.equal(globalStatus(state).barrier, 40);
});

test('未提交行不进入正式库存，abort 整体丢弃', () => {
  let state = applySnapshot(initialState(), snapshot1);
  ({ state } = applyPage(state, page1));
  assert.deepEqual(state.stocks.A.data, { sku: 'A', quantity: 10 });
  assert.ok(state.open.t1);
  assert.ok(state.open.t2);
  ({ state } = applyPage(state, page2));
  assert.deepEqual(state.stocks.A.data, { sku: 'A', quantity: 12 }); // t1 已提交
  assert.deepEqual(state.stocks.B.data, { sku: 'B', quantity: 20 }); // t2 回滚，B 不变
  assert.equal(state.open.t2, undefined);
  assert.equal(state.lastCommitLsn, 46);
});

test('relation 事件切换字段布局，旧 epoch 行不套用新字段', () => {
  let state = applySnapshot(initialState(), snapshot1);
  ({ state } = applyPage(state, [...page1, ...page2]));
  ({ state } = applyPage(state, page3));
  assert.deepEqual(catalogFields(state, 7, 2), ['sku', 'quantity', 'warehouse']);
  assert.deepEqual(state.stocks.C.data, { sku: 'C', quantity: 5, warehouse: 'east' });
  // A 来自 epoch 1，合并保留既有两列、不凭空补 warehouse
  assert.deepEqual(state.stocks.A.data, { sku: 'A', quantity: 12 });
  assert.equal(state.cursor, 50);
});

test('重放同一事件幂等跳过，不重复物化', () => {
  let state = applySnapshot(initialState(), snapshot1);
  ({ state } = applyPage(state, [...page1, ...page2]));
  const before = structuredClone(state.stocks);
  const result = applyPage(state, page2); // 全部 lsn <= 游标
  assert.equal(result.replayed, 3);
  assert.deepEqual(result.state.stocks, before);
  assert.equal(result.state.cursor, state.cursor);
});

test('非法事件流被拒绝：未知事务的 row/commit/abort', () => {
  let state = applySnapshot(initialState(), snapshot1);
  assert.throws(() => applyEvent(state, { lsn: 41, type: 'row', tx: 'ghost', relation: 7, epoch: 1, values: ['X', 1] }),
    /未 begin/);
  assert.throws(() => applyEvent(state, { lsn: 41, type: 'commit', tx: 'ghost' }), /未知事务/);
  assert.throws(() => applyEvent(state, { lsn: 41, type: 'abort', tx: 'ghost' }), /未知事务/);
});

test('行值数量与 relation 布局不符时拒绝', () => {
  let state = applySnapshot(initialState(), snapshot1);
  state = applyEvent(state, { lsn: 41, type: 'begin', tx: 't1' });
  assert.throws(
    () => applyEvent(state, { lsn: 42, type: 'row', tx: 't1', relation: 7, epoch: 1, values: ['A'] }),
    /字段布局不符/,
  );
});

test('长事务跨多次领取：暂存行随状态保留，提交前货号显示未决', () => {
  let state = applySnapshot(initialState(), snapshot1);
  ({ state } = applyPage(state, page1)); // t1、t2 都还开着
  let view = skuStatus(markConfirmed(markHighWater(state, 50), 40), 'A');
  assert.equal(view.openTransaction, true);
  assert.equal(view.caughtUp, false);
  ({ state } = applyPage(state, page2));
  ({ state } = applyPage(state, page3));
  state = markConfirmed(markHighWater(state, 50), 50);
  view = skuStatus(state, 'A');
  assert.equal(view.openTransaction, false);
  assert.equal(view.caughtUp, true);
  assert.equal(globalStatus(state).caughtUp, true);
});

test('只发生回滚的货号不会被永久卡在未追平', () => {
  let state = applySnapshot(initialState(), snapshot1);
  state = applyEvent(state, { lsn: 43, type: 'begin', tx: 't2' });
  state = applyEvent(state, { lsn: 44, type: 'row', tx: 't2', relation: 7, epoch: 1, values: ['B', 0] });
  state = applyEvent(state, { lsn: 45, type: 'abort', tx: 't2' });
  state = markConfirmed(markHighWater(state, 50), 50);
  assert.equal(skuStatus(state, 'B').openTransaction, false);
  assert.equal(skuStatus(state, 'B').caughtUp, true);
});

test('确认位置不能倒退，高水位也不回退', () => {
  let state = markConfirmed(initialState(), 46);
  assert.throws(() => markConfirmed(state, 40), /不能倒退/);
  state = markHighWater(state, 50);
  assert.equal(markHighWater(state, 10).sourceHigh, 50);
});

// 纯函数式的采集状态机：基线装载与事务事件应用，不做任何 IO，便于测试与重建回放。
//
// 核心不变量：
// - row 事件只进入按事务隔离的暂存区；commit 才物化为正式库存，abort 整体丢弃。
// - 每行按其 relation/epoch 的字段布局解释；旧 epoch 的长事务不会套用新字段。
// - 只推进游标、不回退；lsn 严格递增，重复事件（重放）被跳过，处理天然幂等。

export function initialState() {
  return {
    version: 1,
    initialized: false,
    snapshot: null,            // { name, barrier, relation, stream, receiptId, digest, capturedAt }
    cursor: null,              // 下一次 /changes?after= 的位置
    sourceHigh: null,          // 来源最近一次告知的高水位
    lastCommitLsn: null,       // 已应用（已物化）的最高 commit 位置
    confirmedLsn: null,        // 已被来源确认可靠接收的提交边界
    relations: {},             // { [relationId]: { [epoch]: [fields] } }
    open: {},                  // { [tx]: { beginLsn, rows: [...] } }
    stocks: {},                // { [sku]: { data, appliedLsn, relationId, epoch } }
    rebuilds: 0,
    updatedAt: null,
  };
}

export function relationKey(relationId, epoch) {
  return `${relationId}:${epoch}`;
}

export function catalogFields(state, relationId, epoch) {
  return state.relations[relationId]?.[epoch] ?? null;
}

// 装载一份基线导出：建立字段目录、物化已提交基线行，并把游标定位到 barrier。
export function applySnapshot(state, snapshot, meta = {}) {
  const next = structuredClone(state);
  const { id, epoch, fields } = snapshot.relation;
  next.relations = structuredClone(state.relations);
  next.relations[id] ??= {};
  next.relations[id][epoch] = [...fields];

  next.stocks = {};
  const skuIndex = fields.indexOf('sku');
  for (const values of snapshot.rows) {
    const data = Object.fromEntries(fields.map((field, index) => [field, values[index]]));
    const sku = values[skuIndex];
    next.stocks[sku] = { data, appliedLsn: snapshot.barrier, relationId: id, epoch };
  }

  next.open = {};
  next.snapshot = {
    name: snapshot.snapshot,
    barrier: snapshot.barrier,
    relation: structuredClone(snapshot.relation),
    stream: snapshot.stream,
    receiptId: meta.receiptId ?? null,
    digest: meta.digest ?? null,
    capturedAt: meta.capturedAt ?? new Date().toISOString(),
  };
  next.cursor = snapshot.barrier;
  next.sourceHigh = Math.max(next.sourceHigh ?? snapshot.barrier, snapshot.barrier);
  next.lastCommitLsn = null;
  next.confirmedLsn = null;
  next.initialized = true;
  next.updatedAt = new Date().toISOString();
  return next;
}

// 应用单个事务事件。lsn 不大于游标的事件视为重放，原样返回状态。
export function applyEvent(state, event) {
  if (state.cursor !== null && event.lsn <= state.cursor) return state;
  let next = state;
  switch (event.type) {
    case 'begin':
      if (next.open[event.tx]) throw new Error(`事务 ${event.tx} 重复 begin，事件流不合法`);
      next = { ...next, open: { ...next.open, [event.tx]: { beginLsn: event.lsn, rows: [] } } };
      break;
    case 'row': {
      const txn = next.open[event.tx];
      if (!txn) throw new Error(`收到未 begin 的事务行：tx=${event.tx} lsn=${event.lsn}`);
      const fields = catalogFields(next, event.relation, event.epoch);
      if (!fields) throw new Error(`缺少 relation ${event.relation} epoch ${event.epoch} 的字段定义`);
      if (event.values.length !== fields.length) {
        throw new Error(`行值数量与 relation ${event.relation} epoch ${event.epoch} 字段布局不符`);
      }
      next = {
        ...next,
        open: {
          ...next.open,
          [event.tx]: {
            ...txn,
            rows: [...txn.rows, { lsn: event.lsn, relation: event.relation, epoch: event.epoch, values: [...event.values] }],
          },
        },
      };
      break;
    }
    case 'abort': {
      if (!next.open[event.tx]) throw new Error(`abort 了未知事务：tx=${event.tx} lsn=${event.lsn}`);
      const openAfter = { ...next.open };
      delete openAfter[event.tx];
      next = { ...next, open: openAfter };
      break;
    }
    case 'commit': {
      const txn = next.open[event.tx];
      if (!txn) throw new Error(`commit 了未知事务：tx=${event.tx} lsn=${event.lsn}`);
      next = materialize(next, txn, event);
      const openAfter = { ...next.open };
      delete openAfter[event.tx];
      next = { ...next, open: openAfter, lastCommitLsn: event.lsn };
      break;
    }
    case 'relation': {
      const relations = structuredClone(next.relations);
      relations[event.relation] ??= {};
      relations[event.relation][event.epoch] = [...event.fields];
      next = { ...next, relations };
      break;
    }
    default:
      throw new Error(`未知事件类型：${event.type}`);
  }
  next = { ...next, cursor: event.lsn, updatedAt: new Date().toISOString() };
  return next;
}

// 提交时把该事务暂存的行按 lsn 顺序物化为正式库存。
function materialize(state, txn, commit) {
  let stocks = structuredClone(state.stocks);
  for (const row of txn.rows) {
    const fields = catalogFields(state, row.relation, row.epoch);
    const skuIndex = fields.indexOf('sku');
    const key = row.values[skuIndex];
    const incoming = Object.fromEntries(fields.map((field, index) => [field, row.values[index]]));
    // 字段级合并：保留本行布局之外的既有字段（如旧 epoch 行不携带新增列）。
    const previous = stocks[key]?.data ?? {};
    stocks[key] = {
      data: { ...previous, ...incoming },
      appliedLsn: row.lsn,
      relationId: row.relation,
      epoch: row.epoch,
    };
  }
  return { ...state, stocks };
}

// 一页事件按顺序应用，返回新状态与被跳过的重放事件数量。
export function applyPage(state, events) {
  let next = state;
  let replayed = 0;
  for (const event of events) {
    const before = next.cursor;
    next = applyEvent(next, event);
    if (next.cursor === before) replayed += 1;
  }
  return { state: next, replayed };
}

// 记录来源高水位与确认位置，不改变任何业务数据。
export function markHighWater(state, high) {
  if (typeof high !== 'number') return state;
  if (state.sourceHigh !== null && high < state.sourceHigh) return state;
  return { ...state, sourceHigh: high, updatedAt: new Date().toISOString() };
}

export function markConfirmed(state, lsn) {
  if (state.confirmedLsn !== null && lsn < state.confirmedLsn) {
    throw new Error(`确认位置不能倒退：${state.confirmedLsn} -> ${lsn}`);
  }
  return { ...state, confirmedLsn: lsn, updatedAt: new Date().toISOString() };
}

// 某货号是否仍被未决（未 commit/abort）事务触及。
export function skuHasOpenTransaction(state, sku) {
  for (const txn of Object.values(state.open)) {
    for (const row of txn.rows) {
      const fields = catalogFields(state, row.relation, row.epoch);
      const key = row.values[fields.indexOf('sku')];
      if (key === sku) return true;
    }
  }
  return false;
}

// 运营视角：单个货号相对来源的追平情况。
export function skuStatus(state, sku) {
  const record = state.stocks[sku];
  const open = skuHasOpenTransaction(state, sku);
  const appliedLsn = record ? record.appliedLsn : null;
  const durable = state.confirmedLsn;
  // 追平 = 正式数据已可靠接收，且没有未决事务，且已领取到来源高水位。
  const caughtUp = Boolean(record) && !open &&
    durable !== null && appliedLsn !== null && appliedLsn <= durable &&
    (state.sourceHigh === null || durable >= state.sourceHigh);
  return {
    sku,
    present: Boolean(record),
    record: record ? structuredClone(record.data) : null,
    appliedLsn,
    confirmedLsn: durable,
    sourceHigh: state.sourceHigh,
    openTransaction: open,
    caughtUp,
  };
}

export function globalStatus(state) {
  const durable = state.confirmedLsn;
  const openTransactions = Object.keys(state.open).length;
  const fetchedToHigh = state.sourceHigh === null || durable !== null && durable >= state.sourceHigh;
  return {
    initialized: state.initialized,
    barrier: state.snapshot?.barrier ?? null,
    cursor: state.cursor,
    sourceHigh: state.sourceHigh,
    lastCommitLsn: state.lastCommitLsn,
    confirmedLsn: durable,
    openTransactions,
    skuCount: Object.keys(state.stocks).length,
    rebuilds: state.rebuilds,
    caughtUp: state.initialized && openTransactions === 0 && fetchedToHigh,
    snapshot: state.snapshot ? { name: state.snapshot.name, capturedAt: state.snapshot.capturedAt } : null,
    updatedAt: state.updatedAt,
  };
}

// 采集编排：首次基线接入 → 持续事务事件领取 → 提交边界确认 → 过期重建。
//
// 每个轮次的崩溃安全顺序：
//   1. 领取原始页并收存（Archive + journal 先落盘）
//   2. 纯函数应用事件，更新物化状态
//   3. fsync 持久化状态检查点
//   4. 仅对已持久化的 commit 边界调用 /confirm（确认的是可靠接收位置，不是最后见到的事件）
// 任何一步崩溃，重启后都从已持久化游标重放，重复事件幂等跳过，不会漏也不会把未提交/回滚数据转正。

import { ExpiredPositionError, Gateway, GatewayError } from './gateway.mjs';
import { CollectorStore } from './store.mjs';
import {
  applyPage, applySnapshot, globalStatus, markConfirmed, markHighWater, skuStatus,
} from './engine.mjs';

export class Collector {
  constructor(base, root, options = {}) {
    this.gateway = options.gateway ?? new Gateway(base);
    this.store = new CollectorStore(root);
    this.running = false;
  }

  // 进程启动入口：恢复检查点（必要时从原始收件重建），首次接入则建立基线。
  async bootstrap() {
    const loaded = await this.store.load();
    if (!this.store.state.initialized) {
      const result = await this.captureSnapshot('initial');
      return { ...result, load: loaded.mode, bootstrap: 'snapshot' };
    }
    return { load: loaded.mode, bootstrap: 'resumed', ...this.describe() };
  }

  // 领取并装载一份基线；reason 区分首次接入与过期重建。
  async captureSnapshot(reason, path) {
    const { bytes, data, source } = await this.gateway.snapshot(path);
    const receipt = await this.store.archiveCapture(bytes, source, { kind: 'snapshot', reason, barrier: data.barrier });
    let state = applySnapshot(this.store.state, data, {
      receiptId: receipt.id, digest: receipt.digest, capturedAt: new Date().toISOString(),
    });
    if (reason === 'expired-rebuild') {
      state = { ...state, rebuilds: state.rebuilds + 1 };
      await this.store.appendJournal({ kind: 'rebuild', reason: 'expired-position', barrier: data.barrier, at: new Date().toISOString() });
    }
    await this.store.persist(state);
    // 基线本身就是已提交数据，其 barrier 是合法的提交确认点。
    await this.confirmBarrier(data.barrier);
    return { bootstrap: reason, barrier: data.barrier, receipt: receipt.id, ...this.describe() };
  }

  // 执行一轮领取与处理；无新事件时返回 idle。
  async runOnce() {
    if (!this.store.state.initialized) throw new Error('采集器尚未建立基线，请先 bootstrap');
    const after = this.store.state.cursor;
    let page;
    try {
      page = await this.gateway.changes(after);
    } catch (error) {
      if (error instanceof ExpiredPositionError) return this.rebuildAfterExpiry(error);
      throw error;
    }

    const { bytes, data, source } = page;
    const receipt = await this.store.archiveCapture(bytes, source, {
      kind: 'page', after, count: data.events.length, high: data.high,
    });

    let state = this.store.state;
    const { state: applied, replayed } = applyPage(state, data.events);
    state = markHighWater(applied, data.high);
    await this.store.persist(state);

    const commitLsns = data.events.filter(event => event.type === 'commit').map(event => event.lsn);
    const confirmed = [];
    for (const lsn of commitLsns) {
      if (state.confirmedLsn !== null && lsn <= state.confirmedLsn) continue;
      await this.acknowledge(lsn);
      confirmed.push(lsn);
    }

    return {
      kind: 'page',
      receipt: receipt.id,
      fetchedFrom: after,
      count: data.events.length,
      replayed,
      next: data.next,
      high: data.high,
      commits: commitLsns,
      confirmed,
      idle: data.events.length === 0,
      ...this.describe(),
    };
  }

  // 保留窗口过期：按来源指引重新建立基线，再从新 barrier 续接。
  async rebuildAfterExpiry(error) {
    const expiredCursor = this.store.state.cursor;
    await this.store.appendJournal({
      kind: 'expired',
      cursor: expiredCursor,
      rebuild: error.rebuild,
      at: new Date().toISOString(),
    });
    const result = await this.captureSnapshot('expired-rebuild', error.rebuild);
    return { kind: 'rebuild', expiredCursor, ...result };
  }

  // 与来源确认一个已可靠落盘的提交边界。
  // 确认失败不影响已持久化的数据：409 时核对来源实际状态，其它错误留给下一轮重试。
  async acknowledge(lsn) {
    try {
      const { data, source } = await this.gateway.confirm(lsn);
      const bytes = Buffer.from(JSON.stringify(data));
      await this.store.archiveCapture(bytes, source, { kind: 'confirm', lsn });
      await this.store.persist(markConfirmed(this.store.state, lsn));
      return { data, reconciled: false };
    } catch (error) {
      if (error instanceof GatewayError && error.status === 409) {
        const remote = await this.gateway.status();
        if (typeof remote.data.confirmed === 'number' && remote.data.confirmed >= lsn) {
          // 来源侧其实已经确认过（例如上次请求在响应丢失前成功），按事实对齐本地状态。
          await this.store.appendJournal({ kind: 'confirm-reconcile', lsn, remote: remote.data.confirmed, at: new Date().toISOString() });
          await this.store.persist(markConfirmed(this.store.state, remote.data.confirmed));
          return { data: { confirmed: remote.data.confirmed }, reconciled: true };
        }
      }
      throw error;
    }
  }

  async confirmBarrier(barrier) {
    if (this.store.state.confirmedLsn !== null && barrier <= this.store.state.confirmedLsn) return null;
    return this.acknowledge(barrier);
  }

  // 持续采集：按间隔轮询直到被中止；信号触发后完成当前轮次再退出。
  async run({ intervalMs = 250, maxRounds = Infinity, signal, onRound = null } = {}) {
    if (this.running) throw new Error('采集循环已在运行');
    this.running = true;
    const rounds = [];
    try {
      let round = 0;
      while (round < maxRounds && !signal?.aborted) {
        const result = await this.runOnce();
        rounds.push(result);
        if (onRound) await onRound(result);
        round += 1;
        if (round >= maxRounds || signal?.aborted) break;
        await delay(intervalMs, signal);
      }
    } finally {
      this.running = false;
    }
    return rounds;
  }

  // 拉取来源实时状态并校准高水位（不推进确认，确认只来自本地可靠接收）。
  async refreshSourceStatus() {
    const { data } = await this.gateway.status();
    await this.store.persist(markHighWater(this.store.state, data.high));
    return data;
  }

  describe() {
    return globalStatus(this.store.state);
  }

  sku(sku) {
    return skuStatus(this.store.state, sku);
  }

  // 全部货号的追平判断，供运营批量巡检。
  skuReport() {
    return Object.keys(this.store.state.stocks).sort().map(sku => this.sku(sku));
  }

  async close() { await this.store.close(); }
}

function delay(ms, signal) {
  return new Promise(resolve => {
    if (signal?.aborted) return resolve();
    const timer = setTimeout(resolve, ms);
    signal?.addEventListener('abort', () => { clearTimeout(timer); resolve(); }, { once: true });
  });
}

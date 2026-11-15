// 采集器本地持久层。
// - state.json：物化检查点，临时文件 + fsync + 目录 fsync + rename 原子替换，进程崩溃不会留下半截状态。
// - journal.ndjson：只追加的采集日志，每收下一份原始资料（基线/变更页/确认回执）就落盘，
//   状态文件缺失或损坏时可凭 Archive 中的原始对象完整重建。
// - 原始字节全部经现有 Archive 按内容寻址收存，保留既有收件凭据能力。

import { randomUUID } from 'node:crypto';
import { mkdir, open, readFile, readdir, rename, unlink } from 'node:fs/promises';
import { join } from 'node:path';
import { Archive } from './archive.mjs';
import { applyPage, applySnapshot, initialState, markConfirmed, markHighWater } from './engine.mjs';

const STATE_NAME = 'state.json';
const JOURNAL_NAME = 'journal.ndjson';

export class CollectorStore {
  #root;
  #state;
  #journal;

  constructor(root) {
    this.#root = root;
    this.archive = new Archive(root);
  }

  get state() { return this.#state; }
  get root() { return this.#root; }

  // 启动入口：优先载入检查点；检查点不可用时从原始收件重建；都没有则返回未初始化状态。
  async load() {
    await mkdir(this.#root, { recursive: true });
    this.#journal = await open(join(this.#root, JOURNAL_NAME), 'a', 0o600);
    try {
      const bytes = await readFile(join(this.#root, STATE_NAME), 'utf8');
      const state = JSON.parse(bytes);
      if (!state || state.version !== 1) throw new Error('状态版本不被识别');
      this.#state = state;
      return { mode: 'checkpoint', state };
    } catch (error) {
      if (error.code !== 'ENOENT') {
        // 损坏的检查点不直接信任，转入重建。
        await this.appendJournal({ kind: 'checkpoint-corrupt', reason: error.message, at: new Date().toISOString() });
      }
      const rebuilt = await this.rebuildFromArchive();
      this.#state = rebuilt;
      await this.persist();
      // 没有任何采集历史时是首次接入；否则是从原始收件重建。
      return { mode: rebuilt.initialized ? 'rebuilt' : 'initial', state: rebuilt };
    }
  }

  // 收存一份原始字节并写一条追加日志，返回收件凭据。
  async archiveCapture(bytes, source, entry) {
    const receipt = await this.archive.save(bytes, source);
    await this.appendJournal({
      ...entry,
      receiptId: receipt.id,
      digest: receipt.digest,
      size: receipt.size,
      at: new Date().toISOString(),
    });
    return receipt;
  }

  async appendJournal(entry) {
    if (!this.#journal) {
      await mkdir(this.#root, { recursive: true });
      this.#journal = await open(join(this.#root, JOURNAL_NAME), 'a', 0o600);
    }
    await this.#journal.writeFile(JSON.stringify(entry) + '\n');
    await this.#journal.sync();
  }

  async persist(next = this.#state) {
    this.#state = next;
    const target = join(this.#root, STATE_NAME);
    const temporary = join(this.#root, '.' + STATE_NAME + '.' + randomUUID());
    const handle = await open(temporary, 'wx', 0o600);
    try {
      await handle.writeFile(JSON.stringify(next, null, 2));
      await handle.sync();
    } finally { await handle.close(); }
    await rename(temporary, target);
    await syncDirectory(this.#root);
  }

  async close() {
    if (this.#journal) { await this.#journal.close(); this.#journal = null; }
  }

  // 依据追加日志顺序重放，配合 Archive 中的原始字节重建内存状态。
  async rebuildFromArchive() {
    let lines;
    try { lines = (await readFile(join(this.#root, JOURNAL_NAME), 'utf8')).split('\n').filter(Boolean); }
    catch (error) { if (error.code === 'ENOENT') return initialState(); throw error; }
    const journal = lines.map(line => JSON.parse(line));
    let state = initialState();
    for (const entry of journal) {
      if (entry.kind === 'snapshot') {
        const payload = JSON.parse((await this.archive.read({ digest: entry.digest, size: entry.size })).toString('utf8'));
        state = applySnapshot(state, payload, { receiptId: entry.receiptId, digest: entry.digest, capturedAt: entry.at });
      } else if (entry.kind === 'page') {
        const payload = JSON.parse((await this.archive.read({ digest: entry.digest, size: entry.size })).toString('utf8'));
        ({ state } = applyPage(state, payload.events));
        state = markHighWater(state, payload.high);
      } else if (entry.kind === 'confirm') {
        state = markConfirmed(state, entry.lsn);
      } else if (entry.kind === 'rebuild') {
        state = { ...state, rebuilds: state.rebuilds + 1 };
      }
      // checkpoint-corrupt 等审计条目不影响状态。
    }
    return state;
  }

  // 运维核对：列出所有原始收件凭据（与历史 Archive 用法兼容）。
  async listReceipts() { return this.archive.list(); }
}

async function syncDirectory(path) {
  const handle = await open(path, 'r');
  try { await handle.sync(); } finally { await handle.close(); }
}

// 仅供测试与极端修复使用：删除损坏检查点后下次 load 即走重建。
export async function discardCheckpoint(root) {
  try { await unlink(join(root, STATE_NAME)); }
  catch (error) { if (error.code !== 'ENOENT') throw error; }
}

// 扫描某目录下是否存在采集器状态（不读取内容）。
export async function hasCollectorState(root) {
  try {
    const names = await readdir(root);
    return names.includes(STATE_NAME) || names.includes(JOURNAL_NAME);
  } catch (error) {
    if (error.code === 'ENOENT') return false;
    throw error;
  }
}

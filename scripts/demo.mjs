import { createProvider } from '../provider/server.mjs';
import { Collector } from '../src/collector.mjs';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

const directory = await mkdtemp(join(tmpdir(), 'collection-'));
const provider = await createProvider();
const show = (title, value) => console.log(`\n== ${title} ==\n` + JSON.stringify(value, null, 2));

try {
  // 1) 首次接入：基线导出 + 原始收存 + barrier 确认
  const collector = new Collector(provider.base, directory);
  const bootstrap = await collector.bootstrap();
  show('首次接入', bootstrap);

  // 2) 持续采集：先领取第一页（t1/t2 跨页未决），模拟长事务期间来源保留窗口收紧
  const firstRound = await collector.runOnce();
  show('持续采集第一轮（长事务 t1/t2 跨页未提交）', {
    fetchedFrom: firstRound.fetchedFrom, count: firstRound.count,
    openTransactions: firstRound.openTransactions, cursor: firstRound.cursor,
  });

  // 3) 续接位置尚在窗口内时来源宣告旧位置过期：下一轮自动 410 → 重建基线
  await fetch(provider.base + '/_control', {
    method: 'POST', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ mode: 'expired' }),
  });
  const rebuild = await collector.runOnce();
  show('保留窗口过期后按指引自动重建', {
    kind: rebuild.kind, expiredCursor: rebuild.expiredCursor, barrier: rebuild.barrier,
    rebuilds: rebuild.rebuilds, confirmedLsn: rebuild.confirmedLsn,
  });
  show('重建后货号 C（新结构 warehouse 字段）', collector.sku('C'));

  // 4) 恢复正常模式继续持续采集直到空闲
  await fetch(provider.base + '/_control', {
    method: 'POST', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ mode: 'normal' }),
  });
  const rounds = await collector.run({ intervalMs: 10, maxRounds: 3 });
  show('重建后的持续采集轮次', rounds.map(({ kind, fetchedFrom, count, commits, confirmed, idle, replayed }) =>
    ({ kind, fetchedFrom, count, commits, confirmed, idle, replayed })));
  show('采集汇总（回滚事务 t2 被丢弃、长事务 t1/t3 提交后转正）', collector.describe());
  show('正式库存', collector.skuReport().map(({ sku, record }) => ({ sku, ...record })));

  // 5) 模拟进程重启：关闭后用同一持久目录重新打开
  await collector.close();
  const restarted = new Collector(provider.base, directory);
  const resume = await restarted.bootstrap();
  show('进程重启恢复', { ...resume, 空闲轮次: await restarted.runOnce() });

  // 6) 运营追平判断
  show('货号追平情况', restarted.skuReport());
  await restarted.close();

  show('来源确认状态', await (await fetch(provider.base + '/status')).json());
  show('网关审计（只记录路径方法体积，无凭据）', provider.audit);
} finally {
  await provider.close();
  await rm(directory, { recursive: true, force: true });
}

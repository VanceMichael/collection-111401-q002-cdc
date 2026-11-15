import { capture } from './client.mjs';
import { Collector } from './collector.mjs';

// 既有入口：领取一次已提交库存基线并保留原始收件，行为保持不变。
export async function receiveCurrent(base, directory) {
  const { bytes, receipt } = await capture(base + '/snapshot', directory);
  const data = JSON.parse(bytes);
  return {
    receipt: receipt.id,
    snapshot: data.snapshot,
    barrier: data.barrier,
    stocks: data.rows.map(values => Object.fromEntries(data.relation.fields.map((field, index) => [field, values[index]]))),
  };
}

// 首次接入：建立持久采集器并完成基线装载与首次确认。
export async function createCollector(base, directory) {
  const collector = new Collector(base, directory);
  const bootstrap = await collector.bootstrap();
  return { collector, bootstrap };
}

// 进程重启后恢复采集器（检查点优先，损坏时从原始收件重建）。
export async function openCollector(base, directory) {
  const collector = new Collector(base, directory);
  const bootstrap = await collector.bootstrap();
  return { collector, bootstrap };
}

// 运营判断某货号是否已经追平来源。
export function inspectSku(collector, sku) {
  return collector.sku(sku);
}

// 全部货号的追平巡检结果。
export function catchUpReport(collector) {
  return {
    source: collector.describe(),
    skus: collector.skuReport(),
  };
}

// 与库存资料网关的全部网络交互：基线导出、事务事件流分页、提交边界确认。
// 保持与 client.mjs 一致的有限大小、无重定向、超时约束。

export class GatewayError extends Error {
  constructor(message, status, body) {
    super(message);
    this.name = 'GatewayError';
    this.status = status;
    this.body = body;
  }
}

// 续接位置已超出来源保留窗口，body 中的 rebuild 指向重新建立基线的入口。
export class ExpiredPositionError extends GatewayError {
  constructor(status, body) {
    super('变更保留窗口已过，需要重新建立基线', status, body);
    this.name = 'ExpiredPositionError';
    this.rebuild = body?.rebuild ?? '/snapshot';
  }
}

const MAX_BYTES = 8 * 1024 * 1024;

async function limitedFetch(url, options = {}) {
  const response = await fetch(url, {
    redirect: 'manual',
    signal: AbortSignal.timeout(5000),
    headers: { 'cache-control': 'no-store' },
    ...options,
    headers: { 'cache-control': 'no-store', ...(options.headers ?? {}) },
  });
  const chunks = [];
  let size = 0;
  for await (const chunk of response.body) {
    size += chunk.length;
    if (size > MAX_BYTES) throw new Error('单份领取超过当前收件上限');
    chunks.push(chunk);
  }
  const bytes = Buffer.concat(chunks);
  let payload = null;
  if (bytes.length) {
    try { payload = JSON.parse(bytes.toString('utf8')); }
    catch (error) { throw new GatewayError(`来源响应不是合法 JSON：${error.message}`, response.status, bytes); }
  }
  return { response, bytes, payload };
}

export class Gateway {
  constructor(base) {
    if (!/^https?:\/\//.test(base)) throw new Error('网关地址必须是 http(s) 绝对地址');
    this.base = base.replace(/\/$/, '');
  }

  // 领取一次已提交基线导出。
  async snapshot(path = '/snapshot') {
    const { response, bytes, payload } = await limitedFetch(this.base + path);
    if (!response.ok) throw new GatewayError(`基线领取失败：${response.status}`, response.status, payload);
    validateSnapshot(payload);
    return { bytes, data: payload, source: sourceMeta(response, this.base + path) };
  }

  // 领取一页事务事件。位置过期时抛出 ExpiredPositionError，由上层触发重建。
  async changes(after) {
    const url = `${this.base}/changes?after=${encodeURIComponent(after)}`;
    const { response, bytes, payload } = await limitedFetch(url);
    if (response.status === 410) throw new ExpiredPositionError(response.status, payload);
    if (!response.ok) throw new GatewayError(`变更领取失败：${response.status}`, response.status, payload);
    validatePage(payload, after);
    return { bytes, data: payload, source: sourceMeta(response, url) };
  }

  // 确认一个已经可靠接收的提交边界；只能确认 commit LSN，且不能倒退。
  async confirm(lsn) {
    const url = `${this.base}/confirm`;
    const { response, payload } = await limitedFetch(url, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ lsn }),
    });
    if (!response.ok) {
      const error = new GatewayError(`提交边界确认被拒绝：${response.status}`, response.status, payload);
      error.lsn = lsn;
      throw error;
    }
    return { data: payload, source: sourceMeta(response, url) };
  }

  // 来源侧状态：它确认的是已可靠接收的事务位置，high 是当前高水位。
  async status() {
    const url = `${this.base}/status`;
    const { response, payload } = await limitedFetch(url);
    if (!response.ok) throw new GatewayError(`来源状态领取失败：${response.status}`, response.status, payload);
    return { data: payload, source: sourceMeta(response, url) };
  }
}

function sourceMeta(response, url) {
  return {
    url,
    status: response.status,
    type: response.headers.get('content-type'),
    etag: response.headers.get('etag'),
  };
}

function validateSnapshot(data) {
  if (!data || typeof data.barrier !== 'number' || !data.relation || !Array.isArray(data.rows) || !data.stream) {
    throw new GatewayError('基线导出缺少 barrier、relation、rows 或 stream 字段');
  }
  validateRelation(data.relation);
}

function validatePage(data, after) {
  if (!data || !Array.isArray(data.events) || typeof data.next !== 'number' || typeof data.high !== 'number') {
    throw new GatewayError('变更页缺少 events、next 或 high 字段');
  }
  let previous = after;
  for (const event of data.events) {
    if (typeof event.lsn !== 'number' || typeof event.type !== 'string') {
      throw new GatewayError('变更事件缺少 lsn 或 type');
    }
    if (event.lsn <= previous) throw new GatewayError('事件流位置不是严格递增');
    previous = event.lsn;
    if (event.type === 'row' && (typeof event.tx !== 'string' || !event.relation || typeof event.epoch !== 'number' || !Array.isArray(event.values))) {
      throw new GatewayError('row 事件字段不完整');
    }
    if ((event.type === 'begin' || event.type === 'commit' || event.type === 'abort') && typeof event.tx !== 'string') {
      throw new GatewayError('事务边界事件缺少 tx');
    }
    if (event.type === 'relation') {
      validateRelation({ id: event.relation, epoch: event.epoch, fields: event.fields });
    }
  }
  if (data.events.length && data.next !== data.events.at(-1).lsn) {
    throw new GatewayError('变更页 next 与最后事件位置不一致');
  }
}

function validateRelation(relation) {
  if (!relation || typeof relation.id !== 'number' || typeof relation.epoch !== 'number' || !Array.isArray(relation.fields) ||
      !relation.fields.includes('sku')) {
    throw new GatewayError('relation 定义不合法，或缺少 sku 主键字段');
  }
}

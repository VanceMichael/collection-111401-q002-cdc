import test from 'node:test';
import assert from 'node:assert/strict';
import { createProvider } from '../provider/server.mjs';
import { ExpiredPositionError, Gateway, GatewayError } from '../src/gateway.mjs';

test('网关地址必须是绝对 http(s) 地址', () => {
  assert.throws(() => new Gateway('127.0.0.1:8080'), /绝对地址/);
  assert.doesNotThrow(() => new Gateway('http://127.0.0.1:8080/'));
});

test('snapshot 与 changes 的响应结构经过校验', async () => {
  const provider = await createProvider();
  try {
    const gateway = new Gateway(provider.base);
    const snapshot = await gateway.snapshot();
    assert.equal(snapshot.data.barrier, 40);
    assert.ok(snapshot.bytes.length > 0);
    const page = await gateway.changes(40);
    assert.equal(page.data.events.length, 3);
    assert.equal(page.data.next, 43);
    assert.equal(page.data.high, 50);
  } finally {
    await provider.close();
  }
});

test('过期位置抛出带 rebuild 入口的 ExpiredPositionError', async () => {
  const provider = await createProvider();
  try {
    await fetch(provider.base + '/_control', {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ mode: 'expired' }),
    });
    const gateway = new Gateway(provider.base);
    await assert.rejects(() => gateway.changes(40), (error) => {
      assert.ok(error instanceof ExpiredPositionError);
      assert.ok(error instanceof GatewayError);
      assert.equal(error.status, 410);
      assert.equal(error.rebuild, '/snapshot');
      return true;
    });
    // 窗口内的位置仍可领取（after=50 已到高水位，返回空页）
    const page = await gateway.changes(50);
    assert.equal(page.data.events.length, 0);
  } finally {
    await provider.close();
  }
});

test('确认非提交位置被网关拒绝并返回 409', async () => {
  const provider = await createProvider();
  try {
    const gateway = new Gateway(provider.base);
    await assert.rejects(() => gateway.confirm(44), (error) => {
      assert.ok(error instanceof GatewayError);
      assert.equal(error.status, 409);
      assert.equal(error.lsn, 44);
      return true;
    });
    const ok = await gateway.confirm(46);
    assert.equal(ok.data.confirmed, 46);
    // 确认不能倒退
    await assert.rejects(() => gateway.confirm(40), /409/);
  } finally {
    await provider.close();
  }
});

test('status 返回来源确认位置与高水位', async () => {
  const provider = await createProvider();
  try {
    const gateway = new Gateway(provider.base);
    const { data } = await gateway.status();
    assert.equal(data.confirmed, 40);
    assert.equal(data.high, 50);
  } finally {
    await provider.close();
  }
});

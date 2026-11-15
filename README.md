# 关系变更流与基线导出的无缝采集

负责领取一次已提交库存基线，按来源字段布局解释记录，并保留原始基线。变更网关独立提供事务事件及确认入口；本服务在基线之上持续采集事务事件，保证不漏调整、不把未提交或回滚数据写成正式库存。

## 运行

使用 Node.js 22。依赖版本已锁定，运行、测试和演示不下载任何数据或软件。运行时只使用 Node.js 内置模块。

- `npm test`：执行本地来源交互、收件存储、事件引擎与采集器测试。
- `npm run build`：逐个检查模块语法。
- `npm run demo`：启动本地来源，演示首次接入、持续采集、长事务跨页/重启、过期重建与货号追平判断，打印真实收件结果后退出。
- `npm start`：在 127.0.0.1:8080 启动本地来源，PORT 可调整。`GET /health` 返回服务与当前来源版本；原有 `/_health` 仍可使用。

容器方式：`docker build -t collection-cdc .`，然后 `docker run --rm --network none collection-cdc`。镜像构建属于环境准备，运行时不访问公网。

## 项目结构

| 文件 | 职责 |
| --- | --- |
| `src/gateway.mjs` | 网关交互：`/snapshot`、`/changes` 分页、`/confirm`、`/status`，含响应结构校验与 410 过期分类 |
| `src/engine.mjs` | 纯函数事件状态机：基线装载、事务暂存、提交物化、回滚丢弃、relation/epoch 字段目录、追平判断 |
| `src/store.mjs` | 本地持久层：原子状态检查点、只追加采集日志，复用 `Archive` 收存全部原始字节，支持从原始收件重建 |
| `src/collector.mjs` | 采集编排：首次接入、持续轮询、提交边界确认、过期 410 自动重建、进程重启恢复 |
| `src/domain.mjs` | 业务入口：保留 `receiveCurrent`，新增采集器创建/打开与货号追平巡检 |
| `src/client.mjs` / `src/archive.mjs` | 有限大小原始领取与内容寻址收件（能力保持不变） |
| `provider/` | 独立的本地来源服务，契约见 `docs/source-contract.md` |

`docs/collection-flow.md` 说明从首次接入到持续采集的完整流程、崩溃与重启语义。演示使用临时目录，正式接入向采集器传入持久目录即可。

## 持久目录布局

```
<目录>/
  objects/<sha256>      原始字节内容寻址对象（快照、每页变更、确认回执）
  receipts/<uuid>.json  每份原始资料的收件凭据与出处
  journal.ndjson        只追加采集日志，记录每次领取/确认及对应凭据
  state.json            原子替换的物化检查点（库存、未决事务、游标、确认位置）
```

`state.json` 缺失或损坏时，采集器按 `journal.ndjson` 顺序从 `objects` 中的原始资料完整重建状态。

本地服务中的机构、记录和文件都是合成业务资料，不使用真实账号、个人信息或第三方站点数据。全部本地测试应在六十秒内结束。

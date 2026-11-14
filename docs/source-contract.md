# 库存资料网关

这是关系数据库变更的本地交付契约，不是数据库网络协议。`GET /snapshot` 返回已提交基线、relation 定义和 barrier；基线包含到 barrier 为止的数据，后续变化从 `/changes?after=位置` 领取。每页最多三个事件，页边界不是事务边界；事务可以交错，relation 事件对后续相同 epoch 的行有效。

lsn 单调递增。只有 commit 使对应事务的行成为正式库存，abort 表示该事务无效。row.values 按其 relation/epoch 的字段解释，旧 epoch 的长事务不会自动变成新字段布局。`POST /confirm` 的 JSON 为 `{ "lsn": 46 }` 这样的已可靠接收提交边界；确认不能倒退，任意事件位置不是合法确认点。`/status` 给出来源确认和当前高水位。

mode=expired 使较早的续接位置返回 410，响应指向重建入口；服务不保证所有旧位置永远可用。

## 本地演练控制

`GET /_health` 返回服务健康状态；`GET /_audit` 仅返回请求路径、方法和体积，不记录凭据。`POST /_control` 接收 JSON，可调整 generation、mode、clock。控制端点只服务于本地演练，不是资料接口，也不是浏览器业务操作入口。每次重新启动服务都会恢复初始数据；生产接入时不部署此控制面。

# 连接 ID 独立于 PolicyPool（2026-11-06）

- 用户授权删除未使用的 PolicyPool，稳定连接 ConnID 单独保留。本次在原 worktree、`3a579b8` 上修改；不是恢复动态连接策略。
- 产品仅 `pkg/remote/connpool/long_pool.go`：longConn 增加一个 uint64 和惰性 ConnID 方法，由 proxyless 在借用连接期间读取。同一物理连接多次借用保持 ID，新建物理连接获取新 ID，普通连接池使用者不调用时不生成 ID。
- 复用既有 NextConnID 与 shortConn 的实现方式，没有增加地址 map、包装连接、锁、后台任务或淘汰策略。
- 原 ConnID 零分配测试覆盖 longConn 与 shortConn；连接池全包 `-race -count=1` 通过。下游真实复用、换连接与关闭验证在移除 PolicyPool 后执行。
- 未修改其他连接池接口；无本地路径 replace。日志在共同工作区 `.cache/meshlib-remove-policypool-20261106/`。

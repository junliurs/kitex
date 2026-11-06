# longConn 补连接稳定身份（ConnID）

背景：内部 kitex 撤除 PolicyPool 透传层（该项按用户决策整体删除，peers 挂起问题随之退场）后，客户端连接身份失去提供方——原先 policyLease 自带 ConnID，LongPool 返回的 longConn 没有。shortConn 一直有懒分配的稳定 ConnID（short_pool.go），longConn 缺对称实现，导致 proxyless 的 UnaryCall.ConnID 归零、复用判定失效（identity 测试 request 1: ID 1 -> 3, reused=true 复现）。

改动：long_pool.go 给 longConn 增加 id 字段与 ConnID() 方法，与 shortConn.ConnID 完全对称——首次访问懒分配 remote.NextConnID()，连接对象在池内复用期间身份保持不变。无其他改动。

验证：内部 kitex 以本提交为新 pin 后，TestUnaryLifecycleConnIdentity 的 long/short 两个子测试通过，proxyless 全量 -race 通过（见内部 kitex 侧本轮记录）。下游 pin 链（内部 kitex → byted/proxyless → kitex_bench）随后依次固定。

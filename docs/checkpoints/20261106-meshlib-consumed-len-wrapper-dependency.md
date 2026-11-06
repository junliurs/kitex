# 固定 ConsumedLen race 包装修复

- 只将 netpoll 的固定远端提交更新到 3a5eb6b，补充校验和；无本地 replace、其他依赖升级或产品代码变化。
- go build ./... 和 codec 定向 race 测试通过，日志在共同工作空间 .cache/meshlib-confirmed-fixes-20261106/cw-*。

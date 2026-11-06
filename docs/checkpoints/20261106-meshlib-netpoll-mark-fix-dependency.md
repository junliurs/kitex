# meshlib 固定 ReadMark 修复依赖

仅把根模块现有 netpoll 远端映射更新到 5ff953d（MarkRead 到 Release 之间保留被复制读取经过的节点），补充该版本的校验和。不修改本仓库生产/测试代码，Header map 复用保留，没有本地路径 replace，没有 tidy 或无关升级。

新依赖下 go build ./...、Header codec race 通过；日志在共同工作空间 .cache/meshlib-review-fixes-20261106/cw-*。其余组件继续按依赖顺序固定提交并组合验证。

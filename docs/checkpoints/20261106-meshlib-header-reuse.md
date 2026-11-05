# meshlib Header map 复用与可移植依赖

保留兼容性起点 136d54e0bcdc728218c4e81a02c7f6881ef40714，继续在原 worktree 的 test/meshlib-local-integration-20261105 分支提交。

产品改动仅 pkg/remote/codec/header_codec.go，与原 POC 239815ef7 逐字节一致：空 map 使用已有 DecodeInto，非空保留合并语义，错误时回滚部分填入。保留原 header_reuse_test.go，没有其他产品修改。

root 与 bthrift 子模块的本地路径 replace 已移除，只保留必要的固定远端 fork 映射，require 不做无关升级。依赖版本：

- github.com/cloudwego/gopkg => github.com/junliurs/cloudwego-gopkg v0.0.0-20261105192813-e9461b4bc999，提交 e9461b4bc9990522821afc9f462add46ee59ac19。

- github.com/cloudwego/netpoll => github.com/junliurs/netpoll v0.7.4-0.20261105194019-7e272a7601a0，提交 7e272a7601a09ab5f3650d07cd7e0623ddb5d584。

两模块的 go mod download/build/test-link 及 codec race 通过。此前同一三项累积版本的九模块组合构建、真实 Native 双向66 RPC、ACL/Header改写/64并发、client缓存fields/keys检查均通过。test-link仅检查测试代码链接。本次没有改 bm3；历史冷启动失败记录保留，当前尚未做新的正式性能验收。

Go1.26.1，GOWORK=off、mod=readonly，GOGC100、不设GOMEMLIMIT/GODEBUG；模块下载、构建缓存和临时文件均在工作区。每一步只涉及本项实现、原测试、必要依赖和checkpoint。凭证只临时复用原仓库helper，不修改原仓库或全局设置。

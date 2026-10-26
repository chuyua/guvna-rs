# Issue #129 — AutoV2 / Shadow / CLI 架构去重复审（2026-10-26）

**工作基线**：`chuyua/guvna-rs` 开发集成 PR [#151](https://github.com/chuyua/guvna-rs/pull/151)，原 HEAD `9915f57`；本次复审提交 `c292fff`、`b62866a`、`ecd8086`、`0850245`。仅审计当前 Rust 开发集成分支，不套用 Go 的旧缺陷结论。

**工程约束**：单用户、低资源、Linux x86_64；不增加 Web/TCP 远程管理；不复制候选重试状态机；所有变更先在 Draft PR 测试，不自动合并主分支。

## 调用关系和去重判断

| 模块及入口 | 实际责任与证据 | 判断 |
| --- | --- | --- |
| `src/config.rs::discover_auto_candidates` | 将链/显式候选统一物化为 `auto.candidates` | 保留：不是另一套请求执行器 |
| `src/routing.rs::resolve_auto_filtered` | Auto Legacy、健康提示、请求能力约束共用最终候选过滤/稳定排序/裁剪 | 保留：唯一 Auto 候选集合 |
| `src/dispatch.rs` 的 `for step in steps` | Chat、Embeddings、Responses 桥接和 Auto 使用同一供应商重试、KeyPool、熔断、deadline、响应验证 | **统一执行器存在**，没有发现 Decider/Telemetry 另写的业务重试循环 |
| `src/auto_decider.rs::{rank,cached_rank}` | 可选排序建议，配置指纹+最多 32 项缓存、限流和失效后回退 | 保留：缓存的是建议排列，不是执行路线 |
| `src/telemetry_rank.rs::rank` | 审批后使用有时效的 SQLite 统计快照对已合格候选重新排序 | 保留：不是 Decider 缓存；不在请求路径读 SQLite |
| `src/auto_health.rs` | 每物理候选的成功/失败迟滞，热更新按物理身份映射 | **已去重**：删除原先在互斥锁之外附加四个原子状态和 CAS 循环的双重同步 |
| `src/dispatch.rs` Shadow 分支 | 仅调用排名建议、记录与基线的差异；不向影子候选发业务请求 | **已去重**：建议成功、容量不足、缺密钥共用一次统计入口 |
| `src/chain_store.rs` + `src/control_socket.rs` | Unix 本地 Socket 命令、chains 文件事务/互斥和热快照发布 | 保留：与公共 API 隔离；没有第二套在线管理端口 |

## 本轮处理的真实重复

1. `c292fff` — AutoHealth：原代码持有候选 `Mutex` 之后仍分别用四组 `Atomic*` 更新惩罚、失败次数、降级标志和最新故障时间，同时有两套 CAS 循环。合并为一个 `Mutex<ProviderState>`，转移和读取状态均在相同锁内。回归保留超时重置、饱和计数、健康迟滞与热更新前后共享候选身份。
2. `b62866a` — Decider：`validate_order` 用 `HashSet` 检查远端排列，`apply_order` 每次缓存命中又把排列打包成临时 JSON 送回同一入口。改为共同的最多 32 位掩码排列校验，消除缓存热路径不必要的 JSON 分配和 HashSet；保留对重复、越界、缺项和 32 项边界的拒绝。
3. `ecd8086` — Auto 路由入口：以前初次选路、操作员上下文下限降级各自复制完整“候选有默认参数/无默认参数”的分支；Decider/Shadow 再次扫描和序列化同一请求以计算 `input_bytes`、上下文估计与能力标志。现请求特征仅扫描一次，候选原有硬过滤和相同的选择入口继续执行。
4. `0850245` — Shadow：成功建议、并发令牌耗尽和选择器缺密钥存在三处相似的 SQLite 观察/计数代码。统一为 `record_shadow_observation`，只记录建议和基线，不运行假设业务请求；补计数与分歧行为测试。

## 刻意不做的“伪去重”

- **不合并** AutoRouteCache 与 AutoDecider 缓存：前者缓存**过滤后的可执行候选列表**，后者缓存**外部排名顺序**，失效条件、容量和一致性要求不同。硬拼只会产生胶水状态机。
- **不合并** 供应商级 Circuit 与 KeyPool 和模型级 AutoHealth：前者观测物理供应商故障，KeyPool 管理单凭据冷却，AutoHealth 评价具体模型。三者的故障归因不可互相替代。
- **不复制** Chat、Responses、Embeddings 各自的 retry/walk；保持唯一 `dispatch` 循环，不将仅适配格式的转换器误当执行器。
- **不移植** Go 的多套在线管理及远程端口。

## 仍需严格验收（不要靠行数判断）

- `dispatch.rs` 仍耦合候选排序、执行、回退和响应资源租约；拆文件只有在有独立、可测试边界时才做，严禁新建第二条 walk。
- 对外部 Decider 的冷启动、超时、畸形 JSON、失效排名和并发单飞继续跑现有 Linux 回归；对 AutoHealth 热更新与恢复、Shadow 无干预和静态默认结果执行行为等价验收。
- 对遥测分数与实时健康冲突采用实时健康优先，持续监控 SQLite 快照陈旧、不完整、端点混用问题；不在请求热路径同步查询数据库。
- 缺少真实供应商长期 canary 下的 p95/p99、RSS/CPU 和超时样本时，不能宣称“最终稳定版”；Linux 自动测试全绿仅代表现有门禁通过。
- #129 仍是功能总需求，须在合并 main 且完成主线验收后关闭，不能因为旧 PR 已以 superseded 关闭就宣称功能已发布。

## 验证证据

- [统一开发集成 PR #151](https://github.com/chuyua/guvna-rs/pull/151)
- Rust CI 的 `fmt`、`clippy --all-targets --locked -- -D warnings`、`cargo test --locked`、Release 构建与 Adversarial Linux resource gate 在对应最新提交通过后才标记本轮完成。
- 回归位置：`src/auto_health.rs`、`src/auto_decider.rs`、`src/dispatch.rs` 的 `#[cfg(test)]` 及 `tests/gateway_integration.rs` 的 Shadow/Auto/SSE/CLI 系列端到端测试。

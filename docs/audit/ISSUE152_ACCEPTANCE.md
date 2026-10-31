# Issue #152：101 项验收证据

基线：Go `a640b182cd63e27627c38f9bf6f6e26734158377`；Rust PR151 `1936b53a4eb4c356cb47aa8d734a6063c1973ca8`；修复集中在 [PR153](https://github.com/chuyua/guvna-rs/pull/153)，目标为 PR151 开发分支。

本表不把“检查过源码”写成“真实供应商已验证”。`automated` 是已落地的单元/HTTP mock 回归，最终是否通过以对应 HEAD 的 CI 为准；`ci` 是真实 Ubuntu 进程/SDK/资源门禁；`contract` 是明确且可测的兼容边界；`external` 是本轮环境和未合入/未发布状态下无法代替执行的外部门槛。**不得把本表101条登记误读为101条发布条件全部通过。**

首批 `1e6852c` 的 [CI563](https://github.com/chuyua/guvna-rs/actions/runs/41224500023) 已通过fmt、严格Clippy、全量测试、Release、资源门禁和官方SDK。后续代码还增加了真正的JSON Schema验证和compact；最终证据必须使用PR最新HEAD的CI/artifact，不能沿用首批结论。实际构建checkout/PR HEAD/base保存在 `tested-source.json`，数值分别在 `quality-results.json`、`lowload-results.json`、`sdk-results.json`。PR评论补充最终run/SHA。

计数：automated 59；contract 16；ci 17；external 9。机器可读的相同清单见 [ISSUE152_ACCEPTANCE.json](ISSUE152_ACCEPTANCE.json)，保留原任务描述和逐项证据。

## 逐项登记

| 项目 | 类别 | 证据 / 处理结果 |
| --- | --- | --- |
| A01 | automated | `tests/responses_protocol.rs`：24 格矩阵；Chat→RE 的文本、函数、图片与 JSON/SSE；不支持的控制明确拒绝。 |
| A02 | automated | `tests/gateway_integration.rs`：既有 RE→Chat 的 input、函数往返、usage、finish、图像/音频/文件和流式回归。 |
| A03 | automated | `tests/responses_protocol.rs`：native_extensions_are_forwarded_as_one_original_payload；原生失败状态不误记成功。 |
| A04 | automated | `tests/responses_protocol.rs`；`tests/gateway_integration.rs`：混合协议 503 回退、SSE preflight 与首帧后不重放；合法但不可转换输出归因中立。 |
| A05 | automated | `src/config.rs`；`tests/responses_protocol.rs`：同一供应商不同物理模型可独立声明 chat_protocol / responses_protocol。 |
| A06 | contract | `docs/RESPONSES_COMPATIBILITY.md`：声明驱动，不自动盲探测协议；缺省保留旧行为，已知不兼容提前过滤或 501。 |
| A07 | automated | `tests/responses_protocol.rs`：全局存在 native 但选中 chat_only 链时，原生功能 501，双方均无生成 HTTP。 |
| A08 | automated | `src/routing.rs`；`tests/responses_protocol.rs`：Auto/命名链/前缀执行模型能力过滤；未知能力仍由供应商验证，不伪称已探测全部模型。 |
| A09 | automated | `tests/gateway_integration.rs`；`tests/responses_protocol.rs`：Auto 别名、前缀、命名链与原生模型回归；RE-only Decider 真正改变业务候选顺序。 |
| A10 | automated | `tests/responses_protocol.rs`：2 客户端 × 3 供应商协议 × 2 stream_only × 2 stream = 24 种组合。 |
| A11 | automated | `src/dispatch.rs`；`src/routing.rs`；`tests/responses_protocol.rs`：调用者 token 别名覆盖较大链默认值；预算与实际注入一致；native 默认格式/推理正确映射。 |
| A12 | automated | `tests/responses_protocol.rs`；`tests/gateway_integration.rs`：failed/incomplete/refusal、usage 和工具结构回归；HTTP 错误消息继续脱敏，不透传任意供应商错误文本。 |
| A13 | contract | `docs/RESPONSES_COMPATIBILITY.md`：托管工具/状态/加密项选择 native；Chat 无等价表示则明确错误，未新增伪造本地托管服务。 |
| A14 | automated | `src/chat_responses.rs`；`tests/responses_protocol.rs`：单 choice、实际 usage、不兼容输出 501 且不惩罚供应商；原生 phase/多 message/加密输出不静默降级。 |
| A15 | automated | `tests/responses_protocol.rs`；`.github/workflows/ci.yml`：协议矩阵、真实 HTTP mock、SDK 与故障回归均接入 CI。 |
| B01 | automated | `src/responses_lifecycle.rs`；`scripts/sdk_smoke.py`；`scripts/sdk_smoke.cjs`：GET retrieve 仅访问已绑定账号并归一化逻辑 model。 |
| B02 | automated | `src/responses_lifecycle.rs`；`tests/responses_protocol.rs`：真实 DELETE 204 或删除对象验证成功后才移除响应索引。 |
| B03 | automated | `src/responses_lifecycle.rs`；`tests/responses_protocol.rs`：只有 background:true 创建的响应可取消；供应商最终决定状态语义。 |
| B04 | automated | `scripts/sdk_smoke.py`；`scripts/sdk_smoke.cjs`：真实 input_items 分页参数和 list 解析；不合成历史输入。 |
| B05 | automated | `tests/responses_protocol.rs`：原生 input_tokens 精确转发；Chat-only 501，不返回估算冒充精确值。 |
| B06 | automated | `src/response_routes.rs`；`tests/responses_protocol.rs`：跨 HTTP 请求绑定 provider/model/credential/client；失败不越供应商或换账号。 |
| B07 | automated | `tests/responses_protocol.rs`：已登记 conversation 字符串/单 id 对象，previous/conversation 冲突、未知状态与 null 边界。 |
| B08 | contract | `src/response_routes.rs`；`docs/RESPONSES_COMPATIBILITY.md`：1024 条、1h、按需清理；不持久化敏感内容，重启/过期明确 404；不宣称重启连续托管态。 |
| B09 | automated | `src/response_routes.rs`；`tests/responses_protocol.rs`：ID 冲突原子检查、客户端隔离、未知 ID 无网络、外部加密 item 拒绝。 |
| B10 | automated | `scripts/sdk_smoke.py`；`scripts/sdk_smoke.cjs`；`tests/responses_protocol.rs`：background queued→retrieve→cancel→delete 原生工作流；不从非终态伪造完成；查询中新返回的加密 item 验证后登记并固定原账号回放。 |
| B11 | automated | `tests/responses_protocol.rs`：Chat-only store/background/托管工具无法满足时 501。 |
| B12 | automated | `tests/responses_protocol.rs`：有效终态后才登记；SSE 失败、EOF、客户端取消不产生成功索引。 |
| B13 | contract | `docs/RESPONSES_COMPATIBILITY.md`；`tests/responses_protocol.rs`：SSE created/terminal ID 一致；流终态前管理长任务使用非流式 background 创建取得已验证 ID。 |
| B14 | automated | `tests/responses_protocol.rs`：include/prompt/context_management/reasoning 扩展和 MCP 参数深度相等直通测试。 |
| B15 | automated | `tests/gateway_integration.rs`；`tests/responses_protocol.rs`：stateless output replay、函数结果与原生加密/compaction 回放；原账号不可用时不跨厂商重放。 |
| C01 | automated | `src/responses.rs`；`tests/gateway_integration.rs`：字符串、简化/完整 message、角色与混排 item 的既有解析/回放测试。 |
| C02 | automated | `tests/gateway_integration.rs`；`tests/responses_protocol.rs`：文本/refusal/推理摘要/加密 item；未知非文本内容不以空串成功。 |
| C03 | automated | `tests/gateway_integration.rs`：图片 URL/data URI、四种 detail、交错多段和非法输入回归。 |
| C04 | automated | `src/native_files.rs`；`tests/responses_protocol.rs`：Files 元数据同凭据验证；foreign ID 无生成请求，owned ID 才发给该账号。 |
| C05 | contract | `tests/gateway_integration.rs`；`docs/RESPONSES_COMPATIBILITY.md`：wav/mp3 输入回归；音频输出由 native 供应商处理，Chat 桥接不能等价则拒绝。 |
| C06 | contract | `src/native_files.rs`；`tests/gateway_integration.rs`：内联 PDF/file_data 已测；file_url 走原生；file_id 按账号验证；超限沿用体积门禁。 |
| C07 | contract | `docs/RESPONSES_COMPATIBILITY.md`；`tests/gateway_integration.rs`：原生图像生成/视频等 item 原样转发；非等价 Chat 输出明确拒绝。真实供应商能力仍需 G06。 |
| C08 | automated | `tests/gateway_integration.rs`；`tests/responses_protocol.rs`：strict/parameters、指定和并行函数、工具分片与参数 JSON 回归。 |
| C09 | contract | `docs/RESPONSES_COMPATIBILITY.md`；`tests/responses_protocol.rs`：内建工具/allowed_tools 只走声明的原生接口，网关不自行执行。具体模型托管工具由上游验证。 |
| C10 | automated | `tests/gateway_integration.rs`；`tests/responses_protocol.rs`：完整 function_call/output 回放，重复 call_id/非法参数与截断工具结果回归。 |
| C11 | automated | `tests/responses_protocol.rs`；`tests/gateway_integration.rs`：JSON/SSE failed/incomplete/refusal 不伪装 completed；截断参数保持 incomplete。 |
| C12 | automated | `src/structured_output.rs`；`tests/responses_protocol.rs`：标准 JSON Schema 校验 required/type/enum/refs/大整数；八条协议/stream 路径的真实类型错配回退。 |
| C13 | automated | `src/responses.rs`；`tests/gateway_integration.rs`：nullable 控制、max_output_tokens>=16、采样范围、parallel_tool_calls、top_logprobs 范围。 |
| C14 | contract | `src/responses.rs`；`docs/RESPONSES_COMPATIBILITY.md`：可映射的安全/缓存/service_tier/effort 保留；原生扩展直通；模型特定限制由供应商验证。 |
| C15 | automated | `src/response_validation.rs`；`tests/gateway_integration.rs`：usage 细分/总量一致性、logprobs 不静默丢失、logical model JSON/SSE 一致。 |
| C16 | contract | `docs/RESPONSES_COMPATIBILITY.md`：当前官方字段采用 native 原样透传/受控 Chat 映射/明确不支持表；新增 compact，WebSocket 鉴权后 501。 |
| C17 | ci | `scripts/sdk_smoke.py`；`scripts/sdk_smoke.cjs`；`.github/workflows/ci.yml`：官方 Python3.42/Node7.47 SDK create/retrieve/cancel/delete/list/tokens/compact/双协议 stream，归档 sdk-results.json。 |
| D01 | contract | `src/sse.rs`；`src/native_responses_sse.rs`：跨 chunk、CRLF、心跳、Unicode、事件顺序已有测试；重复 response.created 已拒绝。JSON 对象重复键遵循 serde_json 的最后值语义，不宣称全输入重复键拒绝。 |
| D02 | automated | `tests/gateway_integration.rs`；`tests/responses_protocol.rs`：缺终帧、重复终帧、提前 DONE、截断工具、failed/incomplete/refusal 回归。 |
| D03 | automated | `tests/gateway_integration.rs`；`tests/responses_protocol.rs`：首事件 preflight 可回退；下游开始后不再调用备用供应商。 |
| D04 | automated | `src/chat_aggregate.rs`；`tests/gateway_integration.rs`；`tests/responses_protocol.rs`：Chat/原生聚合上限、choices/tool index、JSON/schema、坏流回退与合法终态。 |
| D05 | automated | `tests/gateway_integration.rs`：请求体、总 walk、聚合 idle/lifetime、SSE progress 和准入取消已有对抗回归。 |
| D06 | automated | `tests/gateway_integration.rs`；`src/circuit.rs`：#142 慢 native JSON / 长流的 probe 续租和恢复回归。 |
| D07 | automated | `src/outcome.rs`；`tests/gateway_integration.rs`：401/403/429/5xx、坏2xx、transport/timeout 分类；密钥隔离和健康粒度保持分离。 |
| D08 | automated | `src/keypool.rs`；`tests/gateway_integration.rs`：多密钥各尝试一次、429 Retry-After、租约 epoch、有限候选预算；不进行无界重试。 |
| D09 | automated | `src/chat_stream.rs`；`tests/gateway_integration.rs`；`tests/responses_protocol.rs`：客户端取消、早关/坏流、负载额度释放、凭据健康中立回归。 |
| D10 | automated | `tests/gateway_integration.rs`；`src/telemetry.rs`：已关闭 #143–#149 对应跨模块/SSE/聚合/SQLite/回放回归继续保留。 |
| D11 | ci | `scripts/quality_gate.py`；`src/gateway.rs`：伪造Content-Length、chunk和body上限、嵌套与大payload；压缩传输未启用，8MiB入站/32MiB JSON出站不扩大。 |
| D12 | automated | `src/response_validation.rs`；`src/native_responses_sse.rs`；`tests/gateway_integration.rs`：空/坏JSON、坏SSE、无限心跳、非终态和失败状态不计成功。 |
| D13 | ci | `scripts/quality_gate.py`；`scripts/lowload_gate.py`：128准入、8大payload、4聚合、2大回复、4schema、1后台selector；压力/取消/背压归档。 |
| D14 | automated | `tests/responses_protocol.rs`；`tests/gateway_integration.rs`：全部新增路由鉴权、客户端身份隔离、未知ID无探测；供应商HTTP错误文本脱敏。 |
| D15 | contract | `src/gateway.rs`；`src/native_files.rs`；`docs/RESPONSES_COMPATIBILITY.md`：供应商URL为受信配置；禁重定向，rustls默认校验，文件ID按URL path segment编码；真实TLS/IPv6见G06。 |
| D16 | automated | `src/chat_stream.rs`；`src/telemetry.rs`；`tests/responses_protocol.rs`：终态恰好一次；native failed 与不可转换输出不重置成功计数或错误隔离凭据。 |
| D17 | ci | `src/control_socket.rs`；`src/chain_store.rs`；`src/telemetry.rs`；`scripts/quality_gate.py`：Ubuntu本机Socket、重启链恢复、SQLite锁/事务和CLI门禁；真实systemd/kill重启另见G06。 |
| D18 | ci | `scripts/lowload_gate.py`；`src/dispatch.rs`；`src/structured_output.rs`：同环境A/B及CPU/RSS/fd/线程数据；无同步SQL热路径；schema结构和并发受限，无网络/文件引用。 |
| E01 | automated | `src/routing.rs`；`tests/gateway_integration.rs`：模型列表、未知模型、前缀namespace、命名链与caller-wins；Go逐文件对照见下表。 |
| E02 | ci | `src/main.rs`；`src/control_socket.rs`；`scripts/quality_gate.py`：serve/status/logs/chains、本机Socket权限和JSON输出；不新增远程管理。 |
| E03 | automated | `src/telemetry.rs`；`scripts/quality_gate.py`：SQLite有界异步批量写、失败与健康记录、压满/锁/配额回归。 |
| E04 | automated | `src/auto_decider.rs`；`src/telemetry_rank.rs`；`tests/gateway_integration.rs`：静态回退、TTL缓存、Shadow、显式批准Telemetry模式；原生Decider也复用业务walk。 |
| E05 | automated | `src/model_capabilities.rs`；`src/routing.rs`：Go a640b18 的ctxsize/vision内置表与Rust对应；unknown=8192；显式配置可覆盖，不把估计伪装精确token计数。 |
| E06 | automated | `src/routing.rs`；`tests/gateway_integration.rs`；`tests/responses_protocol.rs`：能力/上下文前置过滤，Auto排序/熔断/恢复和缓存回归。 |
| E07 | contract | `src/keypool.rs`；`docs/RESPONSES_COMPATIBILITY.md`：轮换策略/故障类别对应；不承诺所有Go配置和冷却常数逐字一致，延续既有Rust显式配置契约。 |
| E08 | automated | `src/responses.rs`；`src/chat_responses.rs`；`src/sse.rs`：正反向转换、函数分片/refusal/截断/usage回归；官方协议优先于Go旧桥接限制。 |
| E09 | automated | `src/response_validation.rs`；`tests/gateway_integration.rs`：Embeddings文本、批量、token数组、维度、float/base64及异常向量回归；不启用流式Embedding。 |
| E10 | ci | `src/chain_store.rs`；`src/control_socket.rs`；`scripts/quality_gate.py`：原子落盘、并发在线修改、错误回滚、重启恢复，由Ubuntu文件系统验证。 |
| E11 | ci | `src/metrics.rs`；`src/ops_log.rs`；`scripts/quality_gate.py`：有界日志与无标签指标、低资源/压力观测；不引入Go远程管理历史表面功能。 |
| E12 | contract | `docs/audit/ISSUE152_ACCEPTANCE.md`：固定 Go a640b18 → Rust PR151 1936b53 → PR153最终测试SHA；下面列目录/文件责任对照，不声称Go每条分支逐字等价。 |
| F01 | ci | `.github/workflows/ci.yml`；`scripts/quality_gate.py`：原16/48/6MiB、2000请求64clients门禁不变；近限大payload独立256/128MiB门禁也不放宽。 |
| F02 | ci | `scripts/lowload_gate.py`：串行Chat/RE-bridge/RE-native/Auto，启动和前后闲置CPU/RSS/fd/线程，100请求每通道。 |
| F03 | ci | `scripts/lowload_gate.py`：1/2/4/8/16/64各200混合请求、2000老化；状态码/延迟/RSS均归档。 |
| F04 | contract | `scripts/quality_gate.py`；`scripts/lowload_gate.py`：1MiB原生输入、7MiB近限8并发、12MiB回复与伪32MiB长度；入站8MiB硬上限，超限不承诺接收。 |
| F05 | ci | `scripts/quality_gate.py`；`tests/gateway_integration.rs`：429/5xx/坏2xx/慢流/断流/全候选不可用的有限尝试与内存门禁。真实DNS故障恢复另见G06。 |
| F06 | automated | `src/telemetry.rs`；`scripts/quality_gate.py`：配额、聚合表满、只读/锁/损坏与批量失败单元回归；实际VPS磁盘故障不以mock替代。 |
| F07 | automated | `src/auto_decider.rs`；`src/routing.rs`；`scripts/quality_gate.py`：TTL/LRU容量与后台selector上界、失败冷却、idle CPU门禁。 |
| F08 | ci | `src/dispatch.rs`；`scripts/quality_gate.py`：四聚合名额、大payload/回复准入、schema独立四槽和取消回收；低资源门禁真实进程执行。 |
| F09 | automated | `src/response_routes.rs`；`scripts/lowload_gate.py`：1024条索引/ID长度/TTL/冲突单元回归，1200状态ID洪泛测有界增长。 |
| F10 | ci | `.github/workflows/ci.yml`；`scripts/lowload_gate.py`：同Ubuntu环境构建1936b53与候选Release，保留双方二进制SHA和逐通道p95比值；不是Go性能对比。 |
| F11 | ci | `scripts/lowload_gate.py`；`scripts/quality_gate.py`：p50/p95/p99、CPU/RSS/fd/线程与状态码数值报告；鉴权metrics提供真实Tokio存活任务数/worker数/全局队列深度，混合压测采样峰值，闲置后检查任务数回落。 |
| G01 | external | `https://github.com/chuyua/guvna-rs/issues/152`：技术回归与有限兼容契约逐项记录；最终发布封板仍受G06–G10外部真实证据约束。 |
| G02 | automated | `tests/responses_protocol.rs`；`src/dispatch.rs`：新增真实HTTP失败复现/正常/异常/取消/跨账号测试；翻译模块不拥有独立业务重试循环。 |
| G03 | external | `https://github.com/chuyua/guvna-rs/pull/153`：用户授权独立PR，目标#151；最终集成到#151和main仍待评审，不自动合并。 |
| G04 | ci | `.github/workflows/ci.yml`：Ubuntu最终候选fmt/Clippy/test/Release/version必须同SHA通过；tested-source.json记录实际merge checkout。 |
| G05 | ci | `.github/workflows/ci.yml`：质量、协议SDK、低负载A/B三类artifact及故障测试；以最终HEAD对应CI为准。 |
| G06 | external | `docs/RESPONSES_COMPATIBILITY.md`：真实Linux VPS节点离线，未测试实际供应商/IPv6/TLS/反代/systemd；不得把CI mock勾成VPS canary。 |
| G07 | external | `tested-source.json (CI artifact)`：候选HEAD与CI实际checkout可核验；main合入SHA/发布源码产物需合入后再次核验，当前不存在。 |
| G08 | external | `.github/workflows/manual-linux-build.yml`：main-only发布约束保留；Draft开发PR未合入main，不能虚称可直接Release。 |
| G09 | external | `.github/workflows/manual-linux-build.yml`：手动workflow_dispatch、stable_release=false及ELF/glibc/SHA256/quality核对尚未执行。 |
| G10 | external | `https://github.com/chuyua/guvna-rs/releases`：未发布pre-release；需手动发布后在目标VPS下载、smoke和验证回滚。 |
| G11 | contract | `docs/RESPONSES_COMPATIBILITY.md`：已准备能力矩阵/限制作为发布说明来源；发布时须随实际tag/产物填写，不预先宣称发布成功。 |
| G12 | external | `https://github.com/chuyua/guvna-rs/issues/129`；`https://github.com/chuyua/guvna-rs/issues/152`：保持开放；仅在main合入、Linux验收与发布证据完整时关闭。 |
| G13 | external | `docs/RESPONSES_COMPATIBILITY.md`：稳定版需要另行真实运行证据；本轮交付开发修复和pre-release验收基础，不宣称生产稳定。 |


## 固定 Go SHA 的功能责任对照

| Go 文件 / 目录（a640b18） | Rust 对应文件 | 结论与差异 |
| --- | --- | --- |
| internal/config/config.go | src/config.rs | provider/chain/key/rotation/timeout对应；Rust严格校验，并新增按模型协议声明；不承诺逐字配置兼容。 |
| internal/router/router.go、chainname.go | src/routing.rs、chain_store.rs | named chain、provider prefix、runtime原子修改、caller-wins；token别名覆盖已修正为统一调用者优先。 |
| internal/server/server.go、attempts.go、retry.go、reliability.go | src/dispatch.rs、outcome.rs、circuit.rs | 请求候选/密钥/错误分类、有限重试、熔断和总预算；Rust共用一条业务walker。 |
| internal/server/aggregate_*、stream_reader.go、stream_limits_test.go | src/chat_aggregate.rs、chat_stream.rs、sse.rs | SSE→JSON/流终态/取消/资源边界；Rust另增native Responses聚合。 |
| internal/responses/translate.go、render.go、types.go | src/responses.rs、responses_sse.rs、chat_responses.rs | Go基线主要是Chat桥接；Rust新增反向和原生分支，以官方协议为准，不把旧桥接局限当目标。 |
| internal/ctxsize/builtin.go、registry.go；internal/vision/* | src/model_capabilities.rs、routing.rs | 已核对内置表和8192缺省；Rust用显式能力覆盖配置代替Go外部注册表文件加载方式。 |
| internal/selector/selector.go、server/auto_* | src/auto_decider.rs、auto_health.rs、telemetry_rank.rs、routing.rs | 静态回退、缓存、Shadow、健康/遥测排序；Rust以显式批准控制遥测生效，非同一旧算法逐字复制。 |
| internal/keypool/keypool.go、lease_test.go | src/keypool.rs | round-robin/least-used/sequential、失败分类、lease fencing；Rust保留自身冷却配置契约。 |
| internal/server/embedding_contract.go、embedding_routes_test.go | src/response_validation.rs、dispatch.rs | 文本/批量/token数组、dimensions、encoding_format和向量完整性已有测试。 |
| internal/telemetry/* | src/telemetry.rs、scoring.rs、telemetry_rank.rs | 异步批量记录、容量/配额/损坏/锁/聚合/重启测试；不把同步SQL搬入Tokio请求路径。 |
| internal/localadmin/*、internal/cli/*、internal/logring/* | src/control_socket.rs、main.rs、ops_log.rs | 本机管理和有界日志；不增加远程管理端口。 |
| internal/chains/* | src/chain_store.rs、gateway.rs | 原子落盘、并发修改一致性、live snapshot与失败回滚。 |

## 手动封板清单

1. 确认PR153最终CI三类artifact及实际checkout SHA；对照[兼容契约](../RESPONSES_COMPATIBILITY.md)。
2. 评审后集成到PR151，复核最新main/其他PR冲突；合入main后重新验证实际发布提交。
3. 在真实Linux x86_64 VPS执行有限canary：IPv6-only出站、TLS/反代、systemd和本机CLI、退出/重启、SQLite持久态，以及有限真实Chat与RE供应商请求。此处目前没有证据。
4. main上手动触发现有manual-linux-build，保持stable_release=false；核对ELF、glibc/动态依赖、SHA256、版本、quality-results和源码SHA。
5. 手动创建pre-release并下载产物到目标VPS做smoke/回滚；发布说明引用能力矩阵、索引TTL/重启限制、WebSocket等未支持范围。
6. 只在以上真实证据齐全后关闭#129/#152。稳定版另行验收，不在本修复PR自动宣布。

# Chat / Responses 发布兼容契约

本契约对应 Issue #152、基于 PR #151 的修复 PR #153。它描述可测试的代理行为，不宣称网关实现了 OpenAI 托管工具、存储、模型能力或全部传输方式。所有模型请求仍共用 `dispatch.rs` 的候选、凭据、超时和故障恢复循环。

## 协议声明

| 配置 | 客户端 Chat | 客户端 Responses |
| --- | --- | --- |
| 默认 / `responses_protocol: chat` | 原生 Chat | 受控 RE→Chat 桥接 |
| `responses_protocol: native` | 原生 Chat | 原始 Responses 请求直通 |
| 再加 `chat_protocol: responses` | 受控 Chat→RE 桥接 | 原始 Responses 请求直通 |

协议可以按物理模型覆盖，不能因为同名模型有一个接口就假设另一个也存在。不自动探测、重试不受支持的协议；运营者须正确声明供应商。未声明的功能能力保留旧行为，显式 `supports_*: false` 则在出站请求前过滤，适用于 Auto、命名链和前缀直通。

```yaml
providers:
  - name: mixed
    type: openai
    base_url: https://example.org
    key_env: SUPPLIER_API_KEY
    model_capabilities:
      response-model:
        responses_protocol: native
        chat_protocol: responses
        supports_images: true
        supports_tools: true
        context_tokens: 131072
      text-model:
        responses_protocol: chat
        chat_protocol: chat
        supports_images: false
chains:
  - name: demo
    steps:
      - provider: mixed
        model: response-model
      - provider: mixed
        model: text-model
```

`stream_only: true` 的 Chat 和原生 Responses 均可在四个聚合名额内转成非流式 JSON。原生聚合必须收到并验证真实终态快照，不拼造图片、工具或推理输出。非流式后台任务创建需要可返回 JSON 的原生供应商；不从只流式的后台任务伪造 queued 响应。

可选 Auto Decider 也可使用 `chat_protocol: responses` 的非流式原生供应商。它只收到候选身份和估计摘要，不收到用户正文；失败仍返回静态顺序。

## 字段与输出

| 能力 | RE→Chat | Chat→RE | 原生 Responses |
| --- | --- | --- | --- |
| 文本、system/developer/user/assistant | 转换并保持次序 | 转换并保持次序 | 保留原始 input |
| 图片 URL / data URI、音频输入、内联文件 | 仅可等价的输入形态 | 仅可等价的输入形态 | 由原生供应商处理 |
| 文件 `file_id` | 不可空替代 | 不猜测上传状态 | 同一凭据先 GET Files 元数据验证；全部验证成功才提交生成请求 |
| 函数声明、指定/必需工具、并行调用和工具结果 | 保留声明和 call_id | 保留声明和 call_id | 原样转发 |
| 托管搜索、MCP、代码执行、图像/音频输出等 | 选择 native 或明确拒绝 | 无 Chat 等价物则明确拒绝 | 不在网关执行，由真实上游处理 |
| `previous_response_id` / `conversation` | 不伪造 Chat 持久态 | Chat `store:true` 不映射为虚假的 RE 托管态 | 仅继续网关已登记的原账号状态 |
| 加密 reasoning / compaction 回放 | 只选原生归属账号 | 不丢弃加密项后伪装为 Chat 成功 | 返回的加密 item ID 登记归属，即使 `store:false` 也能在当前进程内安全回放 |
| JSON object / JSON Schema | 真实 JSON / schema 校验 | 真实 JSON / schema 校验 | 真实 JSON / schema 校验；refusal、工具结果、incomplete 不误判为完整 JSON |
| `include`、`prompt`、`context_management`、新原生字段 | 需要 native 时不送 Chat | 没有等价项则拒绝 | 原样直通，model 按路由替换 |
| 多 choices、logprobs、Chat stop/seed 等非等价控制 | 保留可用 Chat 语义 | 不安静删掉控制后继续调用 | 不硬编码成所有模型均支持 |
| native 原生输出、扩展字段、usage 细分 | 不伪装成 Chat 等价输出 | 保留实际 token 数据；不支持的输出形态返回明确错误 | 保留字段；JSON 与 SSE 的 model 均显示逻辑名称 |

Chat→RE 不承诺把多个 native message、带 phase 的消息、注解、加密推理或任意原生工具输出无损塞进一个 Chat choice。发现合法但不可转换的结果时使用兼容性错误，不隔离凭据、不记供应商坏 JSON。已经开始下游 SSE 后只报告本次流失败，不重新发送到其他供应商。

调用者显式输出 token 限制覆盖链默认的所有 token 别名。Native 默认参数只映射有等价含义的字段；不能映射的默认值使该候选不兼容。Responses 固定单输出，不采纳链的多 choices 默认值。

JSON Schema 使用标准 `jsonschema` 引擎，关闭默认网络/文件解析功能并安装拒绝外部引用的 retriever。支持本地 `$defs` / `$ref` 和精确大整数。schema 最大 1 MiB、16,384 个节点、结构深度 64；同时最多四个编译 schema，租约持续到 JSON 完成或流取消。非法、外部引用或超限 schema 在生成请求前拒绝。网关不把“长得像 JSON”当作符合 schema。

## 原生生命周期

| 路由 | 契约 |
| --- | --- |
| `POST /v1/responses` | 原生 JSON / SSE 或受控 Chat 桥接 |
| `GET /v1/responses/{id}` | 只查询登记的原上游账号；默认 JSON；验证后登记查询中新出现的加密 item，后台任务完成后可按原账号回放 |
| `DELETE /v1/responses/{id}` | 上游真实确认删除后才移除本地响应索引；支持 204 或真实删除对象 |
| `POST /v1/responses/{id}/cancel` | 仅允许网关创建时 `background:true` 的响应；由原生上游决定当前状态能否取消 |
| `GET /v1/responses/{id}/input_items` | 分页查询交给原上游；返回真实 list，不构造假历史 |
| `POST /v1/responses/input_tokens` | 路由至原生计数 API；不以本地估计伪装精确计数 |
| `POST /v1/responses/compact` | 路由至原生压缩 API，完整返回 compaction output 并登记加密项归属 |
| `GET /v1/responses` / WebSocket | 鉴权后明确 501；当前使用 HTTP POST 和 SSE |

每个生命周期路由都要求客户端鉴权和相同客户端身份。索引绑定供应商、物理模型、逻辑模型、凭据名称；不存密钥、用户输入、模型输出或加密内容。响应、conversation 和加密 item 共享 **1,024 条 / 1 小时** 上界，按需清理。不同供应商复用同一 ID 不会覆盖原归属。

边界：索引是当前进程内的有界归属缓存，不是 OpenAI 持久存储替代品。重启、TTL 过期或容量淘汰后明确 404；不能跨账号恢复未知外部 ID。不提供 Conversations 创建服务，只有上游响应中已登记的 conversation ID 可继续。服务端状态不能切换逻辑模型或自动跨厂商迁移。已删除的 response ID 不可继续；此前已交给客户端的无状态加密 item 不因此被伪造为删除。

原生 SSE 的响应/加密项只在真实有效终态后登记。断流、错误或客户端提前关闭不产生成功索引。长任务需要流结束前查询/取消时，使用非流式 `background:true` 创建取得 queued/in_progress ID，再查询原上游；不把尚未验证的流首帧当作持久成功。

文件引用每请求最多 16 个不同 ID；每次元数据验证最多 2 秒，总验证窗口最多 8 秒，并受更短的请求总预算约束。不支持 Files 元数据查询的供应商会收到明确的不支持结果，网关不盲发文件 ID。

## 验收与发布

`tests/responses_protocol.rs` 覆盖客户端端点 × Chat/native/dual × stream_only × stream，以及账号隔离、故障、schema 和 SDK。官方 Python 3.42.0 / Node 7.47.0 用本地真实 HTTP fixture 验证 create/retrieve/delete/cancel/input_items/input_tokens/compact 和流式解析；不调用收费 API。

Ubuntu 24.04 CI 保留既有 2,000 请求 / 64 clients 与 16/48/6 MiB 门禁。新增 `lowload_gate.py` 在同一机器构建 PR151 `1936b53` 和候选 Release，复用线程内 HTTP 客户端，串行每通道 500 次请求，计时不包含逐次 TLS 上下文构造；测双协议、1/2/4/8/16/64 阶梯、2,000 轮混合老化和 1,200 个状态 ID，归档 p50/p95/p99、CPU/RSS/fd/线程数、Tokio 存活任务数/worker数/全局调度队列采样和二进制 SHA256。Linux glibc 默认 mmap 分配阈值设为 8 KiB，减少突发流量后空闲 arena 的保留；尊重现有 MALLOC_MMAP_THRESHOLD_ / GLIBC_TUNABLES 覆盖。任务数通过鉴权 metrics 读取；多线程运行时读数是瞬时快照，压测报告给出采样最大值与闲置后的增长。

CI 的 loopback mock 不是目标 VPS、真实供应商、IPv6/TLS/systemd 或生产稳定性证据。最终待执行门槛见 [Issue152 验收表](audit/ISSUE152_ACCEPTANCE.md)。本 PR 不自动合并 main、关闭 #129/#152 或发布版本。

# Ragent 项目面试复习文档

> 本文档基于对 ragent 项目源码的逐文件阅读（含 README、docs/、全部核心 Java 类、`.st` 提示词模板、Lua 脚本、`schema_pg.sql`、`application.yaml`）整理而成。所有结论以**当前代码（分支 1.0.x）实际实现**为准，与常见 RAG 方案不一致之处、README 宣传与代码不一致之处均已显式标出。

---

## 1. 项目概述

### 1.1 项目定位

Ragent 是一个 **Java 技术栈的企业级 Agentic RAG 平台**，覆盖"文档摄取 → 知识库构建 → 在线问答"完整链路：

- **业务问题**：企业内部知识散落在 PDF / Word / Markdown / 飞书 / 远程 URL 等多种来源中，形成信息孤岛；纯关键词搜不准（口语化表达 vs 标准术语）、单一向量检索覆盖不足（意图识别失败即无召回）、知识问答需要同时回答"静态制度"与"实时业务数据"（如天气、订单、销售额）。
- **核心价值**：
  1. **双通道条件召回**：意图定向检索（准）+ 全局向量检索（兜底覆盖），多通道并行 + 去重 + Rerank 后处理链；
  2. **树形意图识别**：3 类意图（KB 知识检索 / MCP 工具调用 / SYSTEM 系统对话），LLM 对叶子节点打分，置信度不足时**主动引导用户澄清**（歧义引导），而不是硬答；
  3. **查询理解链**：术语归一化（数据库 + Redis 缓存的同义词映射）→ LLM 改写 + 多问句拆分（指代消解、口语清洗）→ 规则兜底，缓解"口语表达与标准术语不匹配"；
  4. **会话记忆压缩**：滑动窗口保留近 N 轮原文，增量 LLM 摘要（带 lastMessageId 水位线），控制 Token 成本；
  5. **多模型容错**：多供应商候选链 + 三态熔断器 + 流式首包探测，模型故障自动切换，用户无感知；
  6. **MCP 协议集成**：接独立 mcp-server（官方 MCP Java SDK，Streamable HTTP 传输），LLM 提取参数，工具并行执行；
  7. **工程化能力**：分布式公平排队限流、全链路 Trace（TTFT 可观测）、幂等防重、RocketMQ 事务消息异步分块、React 管理控制台。

### 1.2 项目规模（真实数据）

| 维度 | 数据 |
|---|---|
| 后端模块 | 4 个 Maven 模块 + React 前端（共 5 个顶层应用） |
| 后端 Java 文件 | bootstrap 主代码 362（另 7 个测试类）+ infra-ai 约 45 + framework 38 + mcp-server 6 ≈ 457 个（framework 38 为 git 实测，README 宣称 23） |
| 数据库表 | PostgreSQL 21 张业务表（梳理自 schema_pg.sql：t_user/t_sample_question、t_conversation/t_conversation_summary/t_message/t_message_feedback、t_knowledge_* 六张、t_intent_node/t_query_term_mapping、t_rag_trace_run/t_rag_trace_node、t_ingestion_* 四张、pgvector t_knowledge_vector） |
| 一级业务域 | rag（检索问答）、knowledge（知识库）、ingestion（摄取流水线）、admin（后台）、user（认证）、eval（评测）、trace（可观测） |

### 1.3 我的角色与职责

> 占位：后续按简历描述补充。建议覆盖：多通道检索引擎、公平排队限流器、模型路由与熔断、记忆与重写、入库流水线、MCP 集成、全链路 Trace这几个高含金量点。

---

## 2. 整体架构

### 2.1 架构文字图（自下而上）

```
┌────────────────────────── 前端 React18 + TS（Vite，聊天页 + 管理后台 20+ 页面） ──────────────────────────┐
│  EventSource(SSE) ← HTTP 常规 REST(JSON) →  /api/ragent/*（context-path）                                │
└──────────────────────────────────────────────────────────────────────────────────────────────────┬─────┘
                                                                                                  │
┌────────────────────────────────── bootstrap（主应用，端口 9090，业务全部在这层） ──────────────────────┴────┐
│  rag:  controller(RAGChat/Conversation/IntentTree/Trace/QueryTerm/Settings...)                          │
│        service/pipeline ─ StreamChatPipeline（8 阶段 + 3 短路）                                          │
│        core: intent(树+LSM分类) / rewrite(术语+LLM改写) / guidance(歧义澄清)                              │
│              retrieve(多通道引擎+后处理器) / prompt(场景模板+ContextFormatter)                             │
│              memory(JDBC滑窗+增量摘要) / mcp(客户端注册+参数提取) / vector(Milvus/PG 适配)                │
│  knowledge: 知识库/文档/分块 CRUD、上传限流Filter、RocketMQ 分块消费者、定时刷新Job                       │
│  ingestion: IngestionEngine（链式执行 + 环检测 + 条件跳过），6 种节点                                      │
│  admin/user/eval/trace: 后台报表 / Sa-Token 认证 / 评测 / Trace 落库与查询                               │
│  infra 依赖 ↓                                     MCP HTTP → mcp-server(9099，/mcp)                    │
└───────────────┬───────────────────────────────────────────────────────────┬───────────────────────────┘
                │                                                       │
┌───────────────▼───────────────┐  ┌────────────────────────────────────▼─────────────────────────────────┐
│ infra-ai（AI 基础设施层）      │  │ framework（与业务无关的通用底座）                                     │
│ chat: RoutingLLMService       │  │  UserContext/TraceContext(TTL)/幂等AOP/三级异常/Snowflake(Lua)         │
│  + ChatClient×4(百炼/硅基流动 │  │  SseEmitterSender 全局线程安全 SSE 封装，Result 统一响应               │
│  /Ollama/AIHubMix)           │  │  RocketMQ Producer/SemanticCheckListener 生产者封装                  │
│  + ProbeStreamBridge 首包探测 │  │                                                                       │
│ embedding/rerank: 同路由容错  │  │                                                                       │
│ token: 启发式 Token 估算      │  │                                                                       │
└──────┬────────────────────┬───┘  └───────┬───────────────────────────────────────────────────────────────┘
       │                    │              │
┌──────▼─────┐  ┌───────────▼──┐  ┌────────▼─────────────────────────────────────────────────────────────┐
│ PostgreSQL │  │   Redis +    │  │ RocketMQ 5.2（topic: knowledge-document-chunk_topic，事务消息）        │
│ +pgvector  │  │  Redisson    │  │ Milvus 2.6.6（rag.vector.type=milvus 时，可切换 pgvector）            │
│ 21 张表    │  │ 锁/信号量/   │  │ RustFS(S3 兼容对象存储)、mcp-server(9099)、外部 LLM API                │
│            │  │ PubSub/缓存  │  │                                                                       │
└────────────┘  └──────────────┘  └───────────────────────────────────────────────────────────────────────┘
```

### 2.2 模块职责与依赖方向（不可反向依赖）

| 模块 | 职责 | 依赖 |
|---|---|---|
| `framework` | 23 类/10 个横切点：三级异常体系+全局异常拦截、双维幂等、Snowflake、UserContext/TraceContext（TTL）、SseEmitterSender、MQ 生产者封装、Redis Key 序列化、MyBatis-Plus 元数据填充 | Spring/Redis/Redisson/RocketMQ |
| `infra-ai` | 屏蔽模型供应商差异：Chat/Embedding/Rerank 三类客户端、路由+熔断+首包探测、HTTP 工具、启发式 Token 计数 | framework + OkHttp |
| `mcp-server` | 独立进程（9099），官方 SDK 暴露 `/mcp`，内置 weather_query / ticket(销售) / invoice 示例工具 | MCP Java SDK 1.1.2 |
| `bootstrap` | 全部业务；`@MapperScan` 四个包（rag/knowledge/ingestion/user） | framework + infra-ai + MCP client |
| `frontend` | React 18 + TS；`useChat`/`useStreamResponse` hook 处理 SSE 六类事件 | — |

### 2.3 技术栈清单与选型理由

| 组件 | 版本/位置 | 作用 | 选型理由（面试话术） |
|---|---|---|---|
| Java 17 / Spring Boot | 3.5.7 | 运行时 | LTS + 最新 Boot；Record/Switch 模式匹配大量使用 |
| MyBatis-Plus | 3.5.14 + jsqlparser | ORM 21 张表 CRUD、逻辑删除、审计字段自动填充（`MyMetaObjectHandler`） | Java 团队成本最低，分页/ wrappers 效率高；文档域没有复杂 join 需求 |
| PostgreSQL + pgvector | hnsw(vector_cosine_ops)、gin(jsonb) | 主库 + **可选向量库**（`rag.vector.type: pg`） | 单库同时管业务数据与向量，运维零增量；HNSW 1536 维 COSINE |
| Milvus | 2.6.6 | 可选向量库（`rag.vector.type: milvus`，默认） | 数据量大、需要独立向量检索引擎时切换，`@ConditionalOnProperty` 一键换实现 |
| Redis + Redisson | 4.0 | 意图树/术语映射缓存、RLock（摘要并发）、`RPermitExpirableSemaphore`（**许可可过期**的信号量）、RTopic（限流唤醒、SSE 取消广播） | `RPermitExpirableSemaphore` 是"许可租约自动过期防死锁"的关键，`RSemaphore` 做不到 |
| RocketMQ | 5.2 事务消息 | 文档分块异步化（上传事务提交后才投递） | 事务消息保证"DB 状态 pending 与消息"一致；事务回查 `KnowledgeDocumentChunkTransactionChecker` |
| Apache Tika | 3.2.3 | PDF/Word/Excel/PPT 解析 | 一个 API 覆盖多格式；Markdown 走自研 `MarkdownDocumentParser`（保留结构） |
| Sa-Token | 1.43 | 登录/角色（`SaTokenStpInterfaceImpl` 做数据权限） | 比 Spring Security 轻，集成 Redis 后可水平扩展 |
| TransmittableThreadLocal | 2.14.5 | 用户上下文/Trace 上下文/节点栈跨线程池透传 | 普通静态 ThreadLocal 在线程池复用线程时会串号，InheritableThreadLocal 只在"new Thread"时复制 |
| OkHttp | 4.12 | 同步/流式 HTTP 调 LLM，自研 `OpenAIStyleSseParser` 按行解析 SSE | 需要精细控制取消（`call.cancel()`）与流读取 |
| MCP Java SDK | 1.1.2 | 官方 `McpSyncClient`/`McpSyncServer`，Streamable HTTP 双向 | 标准协议，工具可来自任意第三方 MCP Server |
| React + TS | 18 | 前端 22 页面 | — |

### 2.4 一句话总结调用关系（面试背诵版）

> 请求 → Sa-Token 认证 → `UserContextInterceptor` 塞 TTL 用户上下文 → `@IdempotentSubmit`(SpEL 用户级锁) → `RAGChatController` 建立 SSE → `ChatQueueLimiter` 进入公平排队（Redis 信号量 + ZSET + PubSub）→ 拿到许可后 `StreamChatTraceRunner` 开启 trace → `StreamChatPipeline` 依次执行：记忆加载 → 术语归一化+LLM 改写拆分 → 子问题并行意图识别 →（短路：歧义澄清/纯系统对话）→ `RetrievalEngine` 子问题级并行（KB 走 `MultiChannelRetrievalEngine` 双通道+后处理链；MCP 走 LLM 提参+并行调远程工具）→（短路：空召回）→ 场景化 Prompt 组装 → `RoutingLLMService` 候选链 + 三态熔断 + 首包探测 + OkHttp SSE 流 → `StreamChatEventHandler` 推给前端（META/MESSAGE/REJECT/FINISH/CANCEL/DONE）并落库。

---

## 3. 核心业务完整流程（端到端）

### 3.1 在线问答全链路（`StreamingChatPipeline`，代码级）

```
GET /api/ragent/rag/v3/chat?question=&conversationId=&deepThinking=
│ 1. RAGChatController.chat()
│    - new SseEmitter(sseTimeoutMs=300s)
│    - @IdempotentSubmit(key="UserContext.getUserId()")   ← 同一用户并发提交二次请求会被 Redisson tryLock 直接拒绝
│
│ 2. RAGChatServiceImpl.streamChat()
│    - conversationId 为空 → 雪花 ID 生成新会话 ID；taskId = 雪花 ID
│    - callbackFactory.createChatEventHandler(emitter, convId, taskId)
│      → StreamChatEventHandler 构造时立即发 SSE 事件 meta={conversationId,taskId} 注册到 StreamTaskManager
│
│ 3. ChatQueueLimiter.enqueue()
│    ├ globalEnabled=false → 直通 chatEntryExecutor（SynchronousQueue+Abort，拒绝即 handleReject）
│    └ globalEnabled=true  → FairDistributedRateLimiter.acquire(maxWait=15s):
│        a. entry 存活标记（RBucket TTL = 剩余等待 + 5s 缓冲）必须先于 ZADD 入队 —— 防"入队瞬间被并发 claim 误判僵尸"
│        b. ZADD queue:seq 自增序号 → ZSET(队伍,score=入队序号)
│        c. tryAcquireIfReady:  availablePermits>0 时执行 Lua(queue_claim_atomic.lua)
│           - ZRANGE 头部窗口(maxRank+slack=16) ∃ entry 标记 → 存活；否则视为僵尸 ZREM
│           - 存活排名 < maxRank → 返回 {1, 原score}，同步 ZREM+DEL 自身标记出队
│        d. semaphore.tryAcquire(0, leaseSeconds=30s) —— 拿到"带租约的 permitId"
│           · 拿到 → Ticket.state CAS PENDING→GRANTED → onAcquired 包一层 finally release
│           · 队头但无 permit → 按原 score 重入队（保公平位次）
│        e. 未就绪 → scheduleQueuePoll（200ms 固定速率轮询 + RTopic permit_changed 广播唤醒，
│           PollNotifier 用 firing CAS + pendingNotifications 计数合并通知防惊群；
│           广播先本地查 availablePermits()<=0 直接短路本轮，避免无谓扫描）
│    └ 超时/取消 → handleReject：记录用户问题+拒绝回复进消息表（新会话回查标题，兜底截断问题30字），
│       发 reject/finish/done 事件后 complete —— 记录失败也不能阻塞 emitter，否则前端永远收不到 DONE
│
│ 4. 拿到许可后进入 traceRunner.run()
│    - 生成 traceId（雪花），写 t_rag_trace_run(RUNNING)
│    - traceAwareCallback = ForwardingStreamCallback: onFirstContent→记录 USER_TTFT 节点; onFinish→finishRun(耗时)
│    - RagTraceContext.setTraceId/TaskId → 同步执行业务 → finally clear（线程池复用防污染；异步线程靠 TTL 快照）
│
│ 5. StreamChatPipeline.execute(StreamChatContext)
│    Stage1 loadMemory:   memoryService.loadAndAppend —— 两个 memoryLoad 线程并行：
│                         ① 摘要(最新一条, decorateIfNeeded 包 <conversation-summary>) ② 最近 historyKeepTurns*2 条消息
│                         （DESC 取回后时间序，头部若以 assistant 开头则丢弃至首个 user —— 多轮格式合法化）
│                         并 append 用户消息 → 落 t_message + 首条创建会话&LLM 生成标题
│    Stage2 rewriteQuery: MultiQuestionRewriteService.rewriteWithSplit(q, history)
│                         ① QueryTermMappingService.normalize：Redis 术语映射(en=1,matchType=1 精确，按 priority 降序、
│                            source 长度降序) 替换源词→目标词
│                         ② LLM chat(temperature=0.1/topP=0.3, history 只取最近 4 条 user/assistant——过滤 system 摘要省 Token)
│                            输出 {rewrite, should_split, sub_questions[]}
│                         ③ 双兜底：LLM 挂/解析失败 → (归一化问题, [归一化问题])；开关关闭 → 标点规则拆分 [.?？。；;\n]
│    Stage3 resolveIntents: IntentResolver —— 每个子问题 submit 到 intentClassifyExecutor 并行；
│                         DefaultIntentClassifier.classifyTargets：
│                           Redis(ragent:intent:tree, TTL 7 天) miss → DB t_intent_node(enabled&未删) 组树填 fullPath → 回填
│                           prompt/intent-classifier.st 渲染（id/path/description/examples + MCP 节点加 toolId + KB/SYSTEM 类型标识）
│                         过滤 score<0.35(INTENT_MIN_SCORE)，单子问题最多取若干；总意图数超 MAX_INTENT_COUNT=3 时
│                         "每个子问题保底 1 个最高分 + 剩余配额全局按分排序"重建 —— capTotalIntents
│    ─ 短路① handleGuidance: 仅单子问题时；同系统（CATEGORY 级, parent 是 DOMAIN）取每个系统最高分 → ranked
│        ratio = top2/top1:  <(阈值0.8-边距0.15) → 意图明确跳过；问题文本含某系统 DOMAIN 名别名(≥2字) → 跳过
│        落在 [阈值-边距, 阈值) 边界区间 → AmbiguityLLMChecker 二次确认(JSON {ambiguous,reason})
│        最终渲染 guidance-prompt.st：“检索到以下内容：1) 路径A 2) 路径B 请回复数字选择”→ onContent+onComplete 直接结束
│    ─ 短路② handleSystemOnly: 所有子问题都是单条 SYSTEM 意图 → 取节点 promptTemplate（无则 answer-chat-system.st"小码"人设）
│        temperature=0.7 直接 streamChat，不检索
│    Stage6 retrieve: RetrievalEngine.retrieve(subIntents, defaultTopK=10)
│        - ragContextExecutor 按子问题并行 buildSubQuestionContext
│        - 子问题级 topK：取该子问题 KB 意图节点配置的 topK 最大值，否则回退全局
│        - KB: MultiChannelRetrievalEngine.retrieveKnowledgeChannels
│              [阶段1] searchChannels(Srping List 注入) 过滤 isEnabled→排 priority→supplyAsync(ragRetrievalExecutor) 并行
│                · IntentDirectedSearchChannel(prio=1)：KB 意图(≥minIntentScore=0.4) 存在才启用；
│                  IntentParallelRetriever(模板方法, innerRetrievalExecutor) 按意图并行查 collection, topK*(multiplier=2)
│                · VectorGlobalSearchChannel(prio=10) 兜底策略 isEnabled：
│                    全局通道配置关 → 恒启用；意图定向关 → 恒启用（否则无通道可用）；
│                    无任何意图 / 最高置信 < confidenceThreshold(0.6) / 单一中置信意图< supplementThreshold → 启用
│                  取全部 KB collection，CollectionParallelRetriever 并行检索 topK*3
│              [阶段2] executePostProcessors(责任链, 单个失败跳过不中断)：
│                · Deduplication(order=1)：按通道优先级 INTENT_DIRECTED(1)>KEYWORD_ES(2)>VECTOR_GLOBAL(3) 排序，
│                  LinkedHashMap keyed by chunkId(无则 text.hashCode) 保序去重，同 key 保留高分
│                · Rerank(order=10, 由 rag.rerank.enabled 控制)：
│                  RoutingRerankService(同套路容错)→BaiLianRerankClient(qwen3-rerank) 回退 NoopRerankClient
│        - MCP: executeMcpTools —— 每个 MCP 意图 supplyAsync(mcpBatchExecutor) 并行：
│            McpClientToolExecutor(远程, 官方 SDK callTool) 或本地 bean；
│            参数 = LLMMcpParameterExtractor(自定义 paramPromptTemplate 优先) 按 tool schema 提取 JSON + 填 schema default
│            异常 → isError CallToolResult("工具调用异常:...")，不让单工具失败中断整轮
│        - 单子问题 → 直接取其 kb/mcp 上下文；多子问题 → 按 index 渲染 context-format.st 的
│          sub-question-kb-wrapper / sub-question-mcp-wrapper 拼接；intentChunks 合并
│    ─ 短路③ handleEmptyRetrieval: mcpContext/kbContext 均空 → 固定文案“未检索到..”+onComplete 结束
│    Stage8 streamRagResponse:
│        mergeIntentGroup(全子问题 MCP/KB 意图聚合)
│        RAGPromptService.buildStructuredMessages：
│          场景判定 hasMcp/hasKb → KB_ONLY / MCP_ONLY / MIXED
│          模板优先级：单意图且该节点配置 promptTemplate → 意图模板；否则场景默认(answer-chat-*.st)
│          消息序 = [system(场景提示词)] + history(最前是 system 摘要 <conversation-summary>) +
│                   [user: <documents>…</documents>\n\n<tool-data>…</tool-data>\n\n<questions>1.…/或<question>]
│        温度策略: hasMcp ? (0.3, topP 0.8) : (0, topP 1.0) —— RAG 读静态资料要确定性，MCP 面向自然语言数据稍放开
│        LLMService.streamChat → RoutingLLMService：
│          for target in selector.selectChatCandidates(deepThinking ? 过滤 supports-think + 深思模型置顶 : defaultModel 置顶, priority 升序):
│            healthStore.allowCall(熔断 CLOSED/HALF_OPEN 放行) → client.streamChat(request, ProbeStreamBridge(callback))
│            bridge 模式: 首个 onContent/onThinking → probe.complete(SUCCESS) + 其之前的包被缓冲
│            阻塞 awaitFirstPacket(LlmFirstPacketProbe 独立 bean —— 绕开 Spring AOP self-call 盲区, @RagTraceNode(LLM_TTFT)) 60s
│            SUCCESS → commit 缓冲顺序下发，下游正常收流；失败/超时/无内容 → markFailure + handle.cancel() 换下一个候选
│          全挂 → callback.onError("大模型调用失败，请稍后再试...")
│        StreamChatEventHandler：think/response 两种 delta；按 code point 切 messageChunkSize(默认5,线上一行=1);
│        onComplete → 落 assistant 消息(含 thinking_content/duration) → FINISH{messageId,title} → DONE "[DONE]"
│                     → taskManager.unregister → sender.complete
│
│ 6. 停止生成：POST /rag/v3/stop?taskId → StreamTaskManager.cancel
│    Redis bucket:cancel:taskId (TTL 30min) + RTopic ragent:stream:cancel 广播（含本机经 listener 统一处理防重复）
│    cancelLocal：CAS 单次生效 → handle.cancel() → onCancelSupplier 落库已生成部分 → 发 CANCEL+DONE
```

### 3.2 文档入库链路（两条路径）

**A. 快捷通道（`t_knowledge_document`，`process_mode=chunk`）**：
`KnowledgeDocumentController` 上传 → `UploadRateLimitFilter`（在 **multipart 解析之前**用 `RPermitExpirableSemaphore` 限流，防临时文件；满 429）→ S3 存储（RustFS）→ DB 记录 pending → **RocketMQ 事务消息**（`MessageQueueProducer` + local transaction 内改状态；回查 `KnowledgeDocumentChunkTransactionChecker`）→ `KnowledgeDocumentChunkConsumer.onMessage`（手动 `UserContext.set(operator)`，`executeChunk`：解析 `DocumentParserSelector`（markdown 走自研，Tika 其他）→ `ChunkingStrategyFactory` 选策略 → `ChunkEmbeddingService.embedBatch` → `VectorStoreService` 批量写入 + `t_knowledge_chunk` + `chunk_log` 四段耗时）。

**B. 编排通道（`process_mode=pipeline`，`t_ingestion_*` 4 张表）**：见 §4.9。前端可视化连线配置（React IntentEditPage / IngestionPage），node 定义存 `t_ingestion_pipeline_node.json`，链式执行 + 环检测 + 条件跳过 + 每节点 NodeLog。

### 3.3 知识定时刷新链路（URL 型文档）

`KnowledgeDocumentScheduleJob.scan`（10s 扫描，`next_run_time<=now` 且 `lock_until<now`）→ `ScheduleLockManager.tryAcquire`（DB 行级租约：lock_owner + lock_until(900s)）→ `knowledgeChunkExecutor` 异步 → 心跳续租 + 启动/结束校验持有令牌才写状态 → `RemoteFileFetcher`（支持 ETag / If-Modified-Since；内容 hash 与上次一致 → **跳过重新入库**，只记录 exec）→ 有变化才重新分块入库 → 写 `t_knowledge_document_schedule_exec`。另有兜底 Job：每 60s 把 RUNNING 超时文档重置为 FAILED（进程崩溃恢复）。

---

## 4. 核心模块深度拆解

### 4.1 多通道检索引擎

- **接口**：`SearchChannel`（getName/getPriority/isEnabled/search/getType）。Spring `List<SearchChannel>` 注入即注册，**新通道零配置生效**（`bean` 即插件）。`SearchChannelType` 预留 `KEYWORD_ES` 但**当前无实现类**（差异点，见 §6.4）。
- **引擎**：`MultiChannelRetrievalEngine`（@RagTraceNode(multi-channel-retrieval)）
  - 阶段一：`isEnabled(context)` 过滤 → 按 priority 排序 → `CompletableFuture.supplyAsync(ragRetrievalExecutor)`；通道异常**降级为空结果**不中断其他通道；统计日志（通道数/有结果数/Chunk 总数）。
  - 阶段二：后处理器 `List<SearchResultPostProcessor>` 按 order 升序串联，某处理器异常**跳过继续**（责任链的容错约定）。
- **并行检索模板**：`AbstractParallelRetriever<T>`（模板方法）——子类只需 `createRetrievalTask/targetIdentifier/statisticsName`；两个实现：
  - `IntentParallelRetriever`：按意图（→意图节点 `collectionName`）并行；
  - `CollectionParallelRetriever`：全局通道对**所有 KB collection** 并行（`innerRetrievalExecutor`，带 100 长度队列的线程池）。
- **通道启停策略（重要面试点）**：意图置信度决定走单路还是双路——
  - `VectorGlobalSearchChannel.isEnabled`：`意图定向关闭→必开（兜底）`；无任何意图→开；maxScore<0.6（confidenceThreshold）→开；单一意图但中等置信→开（补充）。即**全局检索是"兜底语义"而非"常开第二路"**，是本次重构（docs/refactoring-summary.md）的核心：多通道解决"意图覆盖不足"，同时控制成本。

### 4.2 意图识别体系

- **数据模型**：`t_intent_node`（kb_id、level 0/1/2=DOMAIN/CATEGORY/TOPIC、parent_code、kind 0/1/2=KB/SYSTEM/MCP——注意 DDL 注释里 kind 只写了 0/1，实际枚举三方；top_k、collection_name、mcp_tool_id、prompt_snippet/prompt_template/param_prompt_template）。
- **缓存**：`IntentTreeCacheManager`，Key `ragent:intent:tree`，TTL 7 天，JSON；意图节点增删改时 `clearIntentTreeCache`。**每次问答都全量读缓存并反序列化**（当前实现，见 §8 局限）。
- **分类器**：`DefaultIntentClassifier`（@Qualifier("defaultIntentClassifier") 指定 bean；测试目录还有 `VectorIntentClassifier` 用 embedding 做向量意图的实验代码）——把所有叶子节点渲染进 `intent-classifier.st`（id/path/description/examples，MCP 节点额外 `type=MCP`/`toolId`），LLM 输出 `[{"id","score","reason"}]`；响应做了三层容错：markdown 围栏剥离（`LLMResponseCleaner`）、`{"results":[...]}` 包裹兼容、未知 id 跳过。
- **意图仲裁**：`IntentResolver`——子问题并行（`intentClassifyExecutor`）→ 单个子问题失败降级空意图；`capTotalIntents`：总意图>3 时"每子问题保底最高分 + 剩余按全局分数"（防止多子问题场景意图爆炸拖垮 collection 检索 —— 代码注释明确："防止拉取过多 Collection 导致性能问题"）。
- **接口注释里的双策略差异（要背）**：`IntentClassifier` Javadoc 写了"意图少→单次调用串行；意图多→按 Domain 拆分并行"，**当前只有串行实现 + 子问题级并行**——即并行粒度在"多子问题"，不在"多 Domain"。（差异点，见 §6.4）

### 4.3 歧义引导

`IntentGuidanceService.detectAmbiguity`：
1. 仅处理**单个子问题**且其 KB 候选≥2；
2. 同系统归并：按 "CATEGORY 级（顶级或父为 DOMAIN）" 节点 id 分组取每系统最高分 → `ranked`（不同系统的同名主题，如"数据安全"同时存在于 OA / 保险系统）；
3. 快速通道跳过澄清（省 LLM 调用）：`top2/top1 < 阈值(0.8)-边距(0.15)`；用户问题里显式包含了某系统 DOMAIN 名（规范化后 ≥2 字符匹配）；
4. 边界区间 `[阈值-边距, 阈值)` 才调 `AmbiguityLLMChecker`（temp 0.1，输出 `{ambiguous,reason}`，**失败/格式异常时降级为"视为歧义"**——宁可多问不误答）；
5. 触发澄清时渲染 `guidance-prompt.st`（选项为节点 fullPath），直接短路返回，**用户回复"1,2"后在下一轮被改写模块当作新问题处理**。

### 4.4 查询重写与拆分

`MultiQuestionRewriteService`（实现 `QueryRewriteService`）：
- 流程：术语归一化 → LLM 改写+拆分（`user-question-rewrite.st`：保留专有名词/删除礼貌语/禁止引入"方面维度角度"枚举词/指代词结合历史还原实体/只在多问号或显式列举时才拆分）→ 任何一步失败兜底"归一化问题+单问题"。
- 术语归一化：`t_query_term_mapping`（domain/source_term/target_term/match_type/priority）→ Redis 缓存 miss 回源并回填；排序 = priority 降序 + 源词长度降序（**长词优先替换**，防止"12306 系统"被短词截断匹配）。
- 历史注入仅取最近 4 条 user/assistant（约 2 轮）并剔除 system 摘要（改写只需要指代消解，不需要长上下文 —— 控 Token）。
- 改写前后整链路都有 `@RagTraceNode(query-rewrite)`。

### 4.5 会话记忆

- **读取**（`DefaultConversationMemoryService.load`）：双 future 并行（摘要 + 历史），`memoryLoadExecutor`；任一失败降级（摘要 null / 历史 []）。
- **历史窗口**（`JdbcConversationMemoryStore`）：`historyKeepTurns * 2` 条（一 user 一 assistant），DESC 取回；`normalizeHistory` 把开头的连续 assistant 丢弃到首个 user（保证多轮以 user 起头）。
- **增量摘要**（`JdbcConversationMemorySummaryService.compressIfNeeded`）：
  - 触发：assistant 消息落库后（异步 `memorySummaryExecutor`），会话用户消息数 ≥ `summaryStartTurns`；
  - 并发控制：Redisson `tryLock`+`unlock`（`ragent:memory:summary:lock:userId:conversationId`），抢不到直接放弃（下一次消息会再试）；
  - 水位线：`summary.lastMessageId`（afterId）到"最近窗口最早 user 消息"（cutoffId）之间做增量摘要，**已摘要区间绝不重复摘要**（afterId ≥ cutoffId 直接 return）；
  - 合并：把 `existingSummary` 作为 assistant 消息注入（Prompt 明确："仅用于合并去重，不得作为事实新增来源；若与本轮对话冲突，以本轮对话为准"），输出 ≤ `summaryMaxChars`（线上 200）单行；
  - **Prompt 设计亮点（面试高频）：摘要"绝对禁止记录具体答案"，只记"话题+处理状态+约束"** —— 因为下一轮会实时检索最新文档，若摘要里存了旧答案会与最新文档冲突导致模型困惑（conversation-summary.st 有正反示例）。
  - 注入形态：摘要作为 system 消息包 `<conversation-summary>`（context-format.st summary-wrapper），置于 history 最前。

### 4.6 模型引擎（路由 / 熔断 / 探测）

- **模型选择 `ModelSelector`**：chat/embedding/rerank 三组候选（yaml `ai.chat.candidates` priority 1..5，覆盖 bailian/ollama/aihubmix/siliconflow）。排序键：`是否首选模型（deepThinking 时 = deep-thinking-model 且过滤 supports-thinking）→ priority → id`；构建 target 时 `healthStore.isUnavailable(id)` 直接剔除 —— **熔断模型在候选阶段就被过滤，而不是调用时失败**。
- **三态熔断 `ModelHealthStore`**（单 JVM `ConcurrentHashMap<id, ModelHealth>`）：
  - `CLOSED`：正常放行 `allowCall`；
  - 失败计数 ≥ `failureThreshold(2)` → `OPEN`，`openUntil = now + openDurationMs(30s)`；
  - `openUntil` 到期 → 进入 `HALF_OPEN`，`halfOpenInFlight=true` 后**只放行一个探测请求**（后续 allowCall 返回 false）；探测成功 restore CLOSED；失败回 OPEN 重新计时；
  - 全部使用 `compute()` 原子编辑。
- **执行器 `ModelRoutingExecutor.executeWithFallback`**：同步路径统一模式——client 缺失跳过 / allowCall 拦截 / 调用成功 markSuccess / 异常 markFailure 继续下一候选；全挂抛 `RemoteException`。
- **流式 + 首包探测（最亮点）**：`RoutingLLMService.streamChat` 对每个候选：
  1. `client.streamChat(request, ProbeStreamBridge(callback), target)`；
  2. `ProbeStreamBridge`（本质是对 `StreamCallback` 的**装饰器**）：首包（onContent/onThinking）到达 → `probe.complete(SUCCESS)`；**探测成功前所有事件被缓冲**（bufferOrDispatch + lock）—— 所以用户在这段时间看到的是"无输出"，由 await 成功后 commit 原样回放，失败则静默丢弃切换下一模型，**前端完全无感知、无脏数据**；
  3. `LlmFirstPacketProbe.awaitFirstPacket`（独立 bean，**原因：Spring AOP 不拦截类内 self-call，拆出去才能让 @RagTraceNode(LLM_TTFT) 生效**）阻塞最多 60s；
  4. 结果 SUCCESS / ERROR / TIMEOUT / NO_CONTENT（onComplete 无任何内容也算失败——防"空转成功"）。
- **底层客户端 `AbstractOpenAIStyleChatClient`**（模板方法）：子类 BaiLian/SiliconFlow/AIHubMix/Ollama 只差 provider/URL/鉴权，共享受 `HttpClientConfig` 里双 OkHttpClient（同步短超时 / 流式长超时）。
  - 流式：`StreamAsyncExecutor.submit(modelStreamExecutor, call, callback)` → runAsync 读 SSE 行，`OpenAIStyleSseParser.parseLine(line, gson, reasoningEnabled)` 区分 content / reasoning_content（深思 onThinking）；**流异常结束（没收到 [DONE]）抛 INVALID_RESPONSE**，取消标志优先吞错；线程池拒绝 → `call.cancel()` + onError("流式线程池繁忙")。
  - `StreamSpanCallback` + `RagStreamTraceSupport`：为供应商流调用开 span（父节点正确归属），终态/取消收尾 —— `span.detach()` 注释解释了"先把节点挂上再提交，否则兄弟节点父链错乱"。
- **Rerank**：`RoutingRerankService` 同套路；`NoopRerankClient`（provider=noop）作为兜底候选。

### 4.7 Prompt 管理

- **模板即文件**：`resources/prompt/*.st`，`PromptTemplateLoader` 用 `ConcurrentHashMap` 缓存 + section 缓存；`{slot}` 简单占位符替换（非重量级模板引擎）。**改提示词不需要重启服务**？—— 前提是热加载，当前实现是首载入缓存（差异点：无热刷新，见 §8）。
- **单文件多 section**：`context-format.st` 用 `--- section: name ---` 定义 12 个片段，`renderSection(path, section, slots)` 按需渲染。
- **场景模板选择**（`RAGPromptService.plan`）：KB_ONLY→answer-chat-kb.st；MCP_ONLY→answer-chat-mcp.st；MIXED→answer-chat-mcp-kb-mixed.st；KB 单意图且节点配置了 `promptTemplate` → 意图级覆盖（未命中检索的意图先被剔除；多意图强制默认模板）。
- **结构化消息**：`buildStructuredMessages = system + history(前置摘要) + user(evidence=kb/mcp 两个 tagged 容器 + question)`；多子问题时 `<questions>` 编号列表 + 每个 `<document index>` 块带 `<question>` 对应子问题、`<rules>` 为该意图 `promptSnippet`。
- **answer-chat-kb.st 的工程细节（面试可讲 5 分钟）**：信息边界最高约束（禁外部知识）；不暴露 `<documents>/<rules>` 内部结构；`<rules>` 不得当作事实复述给用户；HTML 表格规则（rowspan/colspan 原样输出、禁止在 Markdown 单元格里写 `<br>`）；链接/图片 Markdown 原样保留、含敏感参数不输出；信息不足的分级话术。

### 4.8 MCP 集成

- **客户端**：`McpClientAutoConfiguration` 读 `rag.mcp.servers`，`HttpClientStreamableHttpTransport`（url 自动补 `/mcp`）→ `McpClient.sync().initialize().listTools()` → 每个远端 Tool 包成 `McpClientToolExecutor` 注册进 `McpToolRegistry`（与本地 bean executor 同一个注册表，**`@PostConstruct` 自动发现 `List<McpToolExecutor>`**）。
- **参数提取**：`LLMMcpParameterExtractor.extractParameters(question, tool, customPromptTemplate)`（节点级 `paramPromptTemplate` 优先）：把 tool 的 JSON schema（类型/必填/默认值/enum）渲染成自然语言 + 用户问题，LLM 输出 JSON；**只取 schema 里声明过的参数名**（防注入任意 key）；解析失败兜底全默认值。
- **执行**：`RetrievalEngine.executeMcpTools` 按 MCP 意图 `supplyAsync(mcpBatchExecutor)`；按 toolId groupingBy 聚合结果；`DefaultContextFormatter.formatMcpContext` 每个 tool 渲染 `<data>` 块 + 节点 `<rules>`（snippet）；工具异常 → `isError=true` 的 CallToolResult，内容进 `<errors>` section，LLM 可解释失败原因。
- **服务端**：mcp-server 独立 Spring Boot（9099），`McpServer.sync().tools(...)` 暴露 `/mcp`；内置三示例工具：weather_query（city/queryType/days 硬编码20城市模拟数据）、ticket、sales。

### 4.9 入库流水线（ingestion）

- **引擎 `IngestionEngine`**：
  - 起始节点 = 不被任何节点 next 引用的节点；执行链沿 `nextNodeId` 推进；
  - **环检测**：validatePipeline 按节点起 DFS 沿 next 链，出现在 path 中即抛 "流水线存在环"；运行期还有 `executedCount > node总数` 双保险；
  - 条件跳过：`ConditionEvaluator.evaluate(context, conditionJson)` 不满足 → `NodeResult.skip("条件未满足")` 记 NodeLog 但不中断链；
  - 每节点 NodeLog（耗时/成功/error/output = NodeOutputExtractor 按 nodeType 提取对应上下文字段）→ 任务级 JSONB 落 `t_ingestion_task.logs_json` + 节点行 `t_ingestion_task_node` —— **故障可精确定位到节点**。
- **6 类节点**：Fetcher（local-file / s3(RustFS) / http-url / feishu 四源，`MimeTypeDetector` 辨型）→ Parser（`DocumentParserSelector`：markdown 走专用解析器；Tika 覆盖 pdf/doc/docx）→ Chunker（策略工厂）→ Enhancer（全文级 LLM 增强：上下文增强/关键词/问题生成/元数据提取，`EnhancerPromptManager` 内置类型系统提示词，支持节点自定义 system/user 模板 + 指定 modelId）→ Enricher（**分块级** LLM enrich：KEYWORDS/SUMMARY/...结果进 chunk.metadata，可附加文档级 metadata）→ Indexer。
- **Indexer 细节**：collection 取 `context.vectorSpaceId.logicalName` 否则默认；维度校验（缺向量/维度不匹配直接 fail）；ensureVectorSpace 幂等建 collection；metadata 带 chunk_index/task_id/pipeline_id/source；**content >65535 截断**；`context.isSkipIndexerWrite()` 时只准备数据由调用方在**同一事务**统一写库+向量（`TransactionOperations` 编程式事务）。
- **分块策略**：
  - `FixedSizeTextChunker`：默认 chunkSize=512 / overlap=128（节点 settings 可覆盖）；
  - `StructureAwareTextChunker`（Markdown 友好）：先统一 `\r\n→\n`（**注释点名：老 Mac \r 残留会导致空行/标题识别失败** —— 踩坑记录）；块类型 HEADING/` ``` `CODE/整行图片或链接 ATOMIC/空行分段 PARA；min/target/max 预算打包，只在**块边界**切分，绝不改写文本（substring 恒等原文）；overlap 仅复制上一 chunk 尾部子串到下一 chunk 开头。

### 4.10 公平排队限流器（本项目最重的单类，约 470 行）

`FairDistributedRateLimiter`（`rag:global:chat` 实例）见 §3.1 步骤 3。补充要点：
- **为什么不用 Redisson `RSemaphore`**：① 无租约——持permit的进程崩溃会永久占坑，`RPermitExpirableSemaphore` 的 permitId 带 leaseSeconds(30s) 自动回收；② 要**跨实例公平**：请求在 Redis ZSET 里按 seq 排队，"队头窗口"（maxRank = 当前可用许可数 + slack=16）内的才允许 claim，避免别的实例的大队尾请求抢跑；
- **僵尸清理**：entry 标记（RBucket TTL=剩余等待+5s 缓冲）先于入队写；Lua 里对头部窗口成员逐个 EXISTS，不存在的 ZREM —— JVM 崩溃后队列条目靠 TTL 自然消失，下一次 claim 把它当僵尸清掉，**不会永久占住队头窗口**（slack=16 让僵尸密集时也能把存活请求推进窗口内）；
- **Ticket 状态机（PENDING/GRANTED/TIMED_OUT/CANCELLED）**：单一 CAS 协调点，终态互斥、回调最多触发一次；
  - `grant`：**先 set permitRef 再 CAS** —— 并发 cancel/timeout 在 CAS 失败路径能看到 permit 并正确释放（防泄漏）；
  - GRANTED 后业务 Runnable 的 finally 负责释放；**cleanup 在 GRANTED 状态绝不释放 permit**（否则业务运行中把许可还回去 = 同一 slot 借给两个请求 —— 注释原文"等价于把同一 slot 让给另一个请求"）；
  - `onAcquiredExecutor.execute` 被拒 → 显式 release + 降级走 onTimeout（业务没跑，不能占坑）；
  - claim 成功但 permit 没拿到 → **按原 score 重入队**（保公平位次），入队后回查 state，已终态自行回滚，防僵尸条目长期占据队头；
- **唤醒机制**：释放/超时/取消都会 `publishQueueNotify()`；`PollNotifier.fire()`：`firing CAS + pendingNotifications` 计数 —— 连续多次广播只做一轮扫描（合并不惊群），且扫描前先查 `availablePermits()<=0` 短路（许可耗尽时下一轮 release 会再敲门）；
- `ChatQueueLimiter` 为 429 **也走完整会话语义**：用户问题 + 拒绝回复都进消息表、新会话补标题（LLM 失败兜底"问题前 30 字"）—— 前端历史里这轮对话是"看得见"的，不是凭空消失。

### 4.11 全链路 Trace

- **入口**：`StreamChatTraceRunner.run` —— traceId、run 行（含 conversationId/taskId/userId/question）、`ForwardingStreamCallback`（onFirstContent → **USER_TTFT** 节点：从 pipeline 开始到推给用户第一个字，反映改写/意图/检索/LLM 首包全部前置开销；onFinish CAS 一次收尾 SUCCESS/ERROR）；
- **节点**：`RagTraceAspect @Around(@annotation(RagTraceNode))`（Order=HIGHEST+10 先于业务）读 `RagTraceContext.currentNodeId()` 作为 parent、push/pop 维护层级栈；采集类名/方法名/入参出参耗时/status；error 截断 `maxErrorLength(1000)`；
- **上下文**：`RagTraceContext` 三把 TTL；`NODE_STACK` 的 `copy(parentValue)` 返回 `new ArrayDeque(parentValue)` **深拷贝** —— TTL 默认 copy 返回引用，并发子任务会共享同一个 Deque 互相 push/pop，父子节点 ID 串挂、trace 层级紊乱（代码注释原话）；
- **流供应商 span**：`RagStreamTraceSupportImpl`/`StreamSpanCallback`：在提交流线程前开 span，同步段结束 `span.detach()` —— "同步部分结束：把节点从当前线程 NODE_STACK 弹出，避免污染兄弟节点的父节点链"；
- TTFT 双指标：LLM_TTFT（首包探测耗时，`LlmFirstPacketProbe`）与 USER_TTFT（用户感知端到端）分开入 `t_rag_trace_node`，`node_type` 区分。

### 4.12 framework 底座

- **三级异常**：`AbstractException`←Client(业务)/Service(服务端)/Remote(三方) + `GlobalExceptionHandler` 统一转 `Result<T>`；`Result`/`BaseErrorCode` 带错误码规范;`errorcode/exception/kb` 还细分业务异常（如 VectorCollectionAlreadyExistsException）。
- **幂等**：`@IdempotentSubmit(SpEL key)` `IdempotentSubmitAspect`：有 key → SpEL 求值做 lockKey；无 key → `path + userId + 参数MD5`；`tryLock` 失败即重复提交抛 ClientException；`app.eval.enabled=true` 时旁路（压测用）。
- **Snowflake**：Redis Lua（`snowflake_init.lua`）分配 workerId，`CustomIdentifierGenerator` 接 MyBatis-Plus 让 DO 上 ID 自动填充（全库主键都是`VARCHAR(20)` 雪花字符串）。
- `SseEmitterSender`：closed AtomicBoolean + 三回调（onCompletion/onTimeout/onError）置位；**complete/completeWithError 各走一次 CAS，天然幂等**；fail 不再抛 —— 流式响应已开始后再触发全局异常处理器会和已写出的响应冲突。
- MQ：`MessageQueueProducer`（适配多 topic）+ `RocketMQProducerAdapter` + `DelegatingTransactionListener`/`TransactionChecker` —— 事务消息半消息/回查抽象。

---

## 5. 关键技术点（面试高频专题）

### 5.1 RAG 检索策略与召回
- 双通道：意图定向（多 collection 并行、topK×2）+ 全局兜底（按置信度阈值/意图缺失条件触发，topK×3）；后处理去重（通道优先级 INTENT(1)>ES(2)>GLOBAL(3)，key=chunkId 蚕食合并取高分）→ Rerank 截 topK。
- 子问题维度再并行一层（`RetrievalEngine`），形成"子问题×通道×collection"三级并行结构。
- **没有 BM25/ES 关键词通道**（类型枚举留了位）—— 精确匹配能力目前完全依赖向量 + rerank（差异点）。

### 5.2 向量库使用
- 维度 1536，度量 COSINE；Milvus：annsField=embedding，searchParams `ef=128`，outputFields id/content/metadata；查询向量先 **L2 归一化**再送检（余弦度量习惯统一）。
- PG 路径：`SET hnsw.ef_search = 200`（会话级），score = `1 - (embedding <=> ?::vector)`（余弦距离→相似度）；索引 `hnsw(vector_cosine_ops)` + `gin(metadata)`；collection 逻辑存 `metadata->>'collection_name'`（单表多库复用）。
- 写入：`PgVectorStoreService` batchUpdate insert；删除按 metadata 条件；update 用 `ON CONFLICT (id) DO UPDATE`。
- `VectorStoreAdmin` ensureVectorSpace（幂等建 collection）；`MilvusConfig`/`@ConditionalOnProperty(rag.vector.type)` 切换实现。

### 5.3 上下文管理
- 记忆：滑窗(4轮×2) + 增量摘要(200字,水位线,分布式锁,禁止存答案) + 标题 LLM(30字,失败兜底"新对话")。
- 改写历史：只带最近 4 条、去 system 摘要（摘要与改写解耦 —— 摘要给"生成"用，历史窗口给"改写指代"用）。
- Prompt 组装：场景模板 + 节点级覆盖 + tagged 容器；多子问题成组对应。

### 5.4 提示词管理
- 全部外置 .st 文件 + section 机制 + 内存缓存；意图节点级 prompt_snippet（`<rules>`，只对所在子问题生效、不得复述给用户）/ prompt_template（整体覆盖）/ param_prompt_template（MCP 提参）三级可配置；
- 温度策略按场景：改写/意图/提参/歧义确认 0.1（确定性抽取），生成 KB 0.0、MCP 0.3，标题 0.7。

### 5.5 并发处理（数字全部来自 ThreadPoolExecutorConfig）
实际**10 个** TTL 线程池（README 宣传 8 个，差异点）：

| Bean | 核心/最大 | 队列 | 拒绝策略 | 用途 |
|---|---|---|---|---|
| chatEntryExecutor | maxConcurrent(10)=max, allowCoreThreadTimeOut | SynchronousQueue | Abort | 排队拿permit后入口 |
| ragContextExecutor | 4C/4C | Sync | CallerRuns | 子问题级并行 |
| ragRetrievalExecutor | 4C/4C | Sync | CallerRuns | 通道级并行 |
| innerRetrievalExecutor | C/2C | LinkedBlocking(100) | CallerRuns | collection 级并行 |
| intentClassifyExecutor | C/2C | Sync | CallerRuns | 子问题意图分类 |
| mcpBatchExecutor | C/2C | Sync | CallerRuns | MCP 工具并行 |
| memoryLoadExecutor | C/2 / max(4,C) | LinkedBlocking(200) | CallerRuns | 摘要+历史双读 |
| memorySummaryExecutor | 1 / max(2,C/2) | LinkedBlocking(200) | CallerRuns | 异步摘要 |
| modelStreamExecutor | C/2 / max(4,C) | LinkedBlocking(200) | Abort | OkHttp SSE 读 |
| knowledgeChunkExecutor | C/2 / max(4,C) | LinkedBlocking(200) | Abort | 分块/调度执行 |

设计逻辑：**IO 密集交互路径用 SynchronousQueue（立即反馈）、CPU/批量路径用有界队列缓冲、两种 Abort（chatEntry/modelStream）都因为"失败要立刻可见"**；CallerRuns 场景是"宁可让提交线程自己干也别丢任务"；全部 `TtlExecutors.getTtlExecutor` 包装 —— UserContext/TraceContext/节点栈透传。

### 5.6 异常处理
- 分层：ClientException（400 语义）/ServiceException（500 语义）/RemoteException（三方）+ 全局拦截；
- 每条并行支路独立 try-catch 降级为空结果（通道、后处理器、子问题意图、MCP 工具、消息加载），**单点失败不放大**；
- 流式链路：ProbeStreamBridge 消化首包前失败、SseEmitterSender 幂等关闭、`StreamChatEventHandler.onError` 走 sender.fail 而不再抛（响应已开始）；
- MQ：事务消息回查；消费者 `UserContext.set/clear` finally。

### 5.7 Token/成本控制（均有代码依据）
1. 记忆：滑窗 8 条 + 摘要 200 字（内容不存答案，天然短）；
2. 改写历史只取 4 条、剔除 system 摘要；
3. `content > 65535` 截断（Milvus 字段限制 & 控制单 chunk 体积）；
4. HeuristicTokenCounterService：ASCII≈4字符/token、其他≈2字符/token、CJK 1字符=1token —— `t_knowledge_chunk.token_count` 落库（chunk 元数据）；
5. INTENT_MAX_COUNT / topK 本身就是上下文成本控制。

### 5.8 幂等与防重
`@IdempotentSubmit`（用户维度 SpEL / 自动 MD5） + SSE 天然"一连接一响应"；任务取消利用 Redis cancel 标记 30min + RTopic 广播，跨实例取消任意节点上的流。

---

## 6. 项目亮点与设计权衡（为什么这么做）

### 6.1 权衡表（面试官最爱问"为什么不用 X"）

| 决策 | 备选 | 为什么选当前方案 |
|---|---|---|
| 自研模型路由层 | Spring AI / LangChain4j | 需要三态熔断+**流式首包探测切换**这种精细控制；SDK 版本迭代快（README 明说"约等于重写"）；路由层的钩子方法（customizeRequestBody/isReasoningEnabledForStream）正好满足 Ollama 本地无鉴权、百炼 enable_thinking 等**供应商方言** |
| LLM 做意图分类 | 纯 embedding 相似度 | test 目录存在 `VectorIntentClassifier`（每节点预计算向量，`IntentNode.embedding` 已@Deprecated）—— 实验后放弃：**树要频繁运营调整（examples/规则随时加），LLM 分类可把"强匹配/系统限定/数量控制/同名歧义"直接写进 prompt**，零样本迁移；embedding 需要维护向量重算。短列表（叶子节点数量可控）下单次 LLM 成本可接受 |
| ZSET 排队 + Lua 队头窗口 | 纯 RPermitExpirableSemaphore / 消息队列排队 | 信号量无公平性（谁抢到算谁），不满足"先来先服务"；MQ 排队把用户请求序列化成消息会丢 SSE 上下文（emitter 绑定在当前节点）；ZSET+score 保全局 FIF O、Lua 保证"出队判定"原子（见队头窗口/slack） |
| entry TTL 标记 + Lua 清僵尸 | 给 ZSET 成员单独设 TTL（Redis 不支持） | Redis ZSET member 不能独立 TTL，只能用旁路 bucket 存活标记；JVM 崩溃后桶 TTL 到期，下一次 claim 的 Lua 把队列里对应条目当僵尸 ZREM |
| 同步阻塞首包探测（await 60s） | 计数器/健康拨测 | 健康拨测是模拟流量且无法感知"这这次请求"的真实质量；探测真实流的第一包，SUCCESS 前缓冲不外吐 —— 用户侧"无感切换"是硬需求 |
| JDBC 直读消息表 | Redis 缓存消息 | 拉链路消息保证与 t_message 强一致（落库即读）；消息表有 (conversation_id,user_id,create_time) 索引，窗口最多 8 条，读放大可忽略；缓存反而引入失效复杂度 |
| upload 在 multipart 前限流 | Controller/AOP | `@Order(HIGHEST)` OncePerRequestFilter 在**解析之前**拒绝，避免临时文件落盘才限流（真正防的是磁盘/CPU 峰值） |
| ProbeStreamBridge 缓冲首包前的全部事件 | 探测失败直接向下游发 error | 若把半截内容吐给用户再切换模型，会出现重复/脏文本；buffer+commit 保证"要么全无要么全有" |
| MCP 由意图驱动（先分类→再调工具→结果进 Prompt） | ReAct 自主 Agent 循环（LLM 自行决定调什么工具） | 可控性/成本/可解释：每轮最多 MAX_INTENT_COUNT 个工具、参数由 schema 约束提取、Trace 能完整记录；自主 Agent 的多轮决策对内部知识场景收益小、时延和不确定性大（差异点：**这是"检索增强型 Agentic"，不是自主规划型 Agent**） |
| 意图树整棵 JSON 存 Redis（TTL 7 天） | 每节点散 key | 读侧一次网络往返拿全树；节点改动时删 key 全量重建——读多写少场景天然合算 |

### 6.2 可以讲的"工程美感"细节
- FairDistributedRateLimiter 的注释密度极高，每一段并发顺序都有为什么：entry 先写后入队、grant 先 set 再 CAS、GRANTED 不释放、claim 后重入队、PollNotifier 合并通知…… 面试直接当"并发设计案例"讲。
- ProbeStreamBridge 的 buffer/commit 与 Ticket 状态机是同一哲学：**终态一次性提交，中间态对外不可见**。
- `LlmFirstPacketProbe`、`ConversationTitleGenerator` 拆 bean 的原因（AOP self-call）能直接回答"Spring AOP 原理与应用边界"。

### 6.3 与常见 RAG/Agent 教科书方案的差异（面试主动说出来加分）
1. **入口先改写+拆分+意图 → 再检索**，而不是"原始 query 直接向量检索"——查询理解是流水线一等公民；
2. **澄清式交互（Guidance）短路整条管线**，教科书 RAG 没有"用户确认"环节；
3. **检索单元是"意图节点→collection"而非单库**：知识隔离靠树形意图做路由，全局向量只是兜底；
4. **记忆摘要"只记话题不记答案"** —— 反常规（多数实现摘要全文），理由见 §4.5；
5. **MCP 工具调用的触发在意图层而非模型层**，无 function-calling 自由循环。

### 6.4 README/宣传与代码不一致清单（诚实核对，被追问时守得住）
| 项 | 宣传 | 实际 |
|---|---|---|
| 专用线程池 | "8 个线程池" | **10 个**（多了 knowledgeChunkExecutor、memoryLoadExecutor；与 knowledge/ingestion 模块配套） |
| 首包探测装饰器 | 名为 `ProbeBufferingCallback` | 类名是 **`ProbeStreamBridge`**，行为一致（ decorating StreamCallback + 缓冲提交） |
| 后处理器 | "去重/版本过滤/分数归一化/Rerank" | 实现只有 **Deduplication、Rerank** 两个（"版本过滤/分数归一化"仅存在于接口 Javadoc 示例） |
| 检索通道 | 暗示 ES 混合召回 | `SearchChannelType.KEYWORD_ES` 只是枚举占位，**无 ES 通道实现** |
| 意图分类 | "串行/按 Domain 并行两种实现" | 当前是**单次调用串行分类 + 子问题级并行**（IntentClassifier Javadoc 描述的是策略设想） |
| framework 规模 | "23 个类覆盖 10 个横切关注点" | git 实测 **38 个类**（含异常/错误码/DTO/MQ 封装），能力清单本身属实 |
| 上下文增强 | 图片有 ingestionPipeline"可视化连线" | 属实（t_ingestion_pipeline_node + React 编辑页），但**条件跳转/环检测在服务端**，前端仅编辑连线 |

---

## 7. 踩坑记录 & 工程问题（按模块，均为代码注释/提交内容可考证）

1. **TTL 集合引用共享**（framework/trace/RagTraceContext）：默认 copy() 传引用导致并发子任务共用 Deque，相互 push/pop 父子节点 ID 串挂、层级紊乱 → 重写 `copy()` 深拷贝。顺带教训：TTL 的 TTLExecutors 只解决"传过去"，解决不了"并发修改共享结构"。
2. **Spring AOP self-call**：RoutingLLMService 内部调 this.awaitFirstPacket、ConversationServiceImpl 私有方法生成标题，`@RagTraceNode` 不生效（变成孤立 root 节点）→ 拆 `LlmFirstPacketProbe`、`ConversationTitleGenerator` 独立 bean。
3. **公平限流一系列竞态**（FairDistributedRateLimiter，都有注释）：
   - entry 标记必须先于入队，否则竞窗口内并发 claim 把新条目当僵尸 ZREM；
   - claim 成功/permit 未得的"按原 score 重入队"，之后必须回查 state 自行回滚，防僵尸条目长期占据队头；
   - grant 先 set permitRef 再 CAS，否则对方（cancel/timeout）CAS 失败路径看不到 permit → 泄漏；permitRef CAS 防双释放；
   - GRANTED 状态下 cleanup 绝不释放 permit（运行中归还=同一 slot 复用）；
   - onAcquiredExecutor 拒绝执行时：业务没跑，必须显式 release + 走 timeout，不能挂死；
   - 超时条目靠 **entry TTL + 5s 缓冲**（时钟漂移容差），Lua 里 slack=16 才能在僵尸密集时把活请求推进窗口。
4. **SSE 429/异常也必须发 DONE**：ChatQueueLimiter 注释 —— "记录 reject 会话失败不能阻塞 emitter，否则前端永远收不到 DONE"；SseEmitterSender 的 complete/completeWithError 靠 CAS 幂等（重复关闭会抛 IllegalStateException）。
5. **流式读取的异常结束**：OkHttp source 读到 EOF 但没有 [DONE] 事件 → 必须当 INVALID_RESPONSE 处理（completed 标志），不能"正常流结尾"静默放行。
6. **结构感知分块的换行灾难**：`\r\n` / 裁 `\r` 残留会让空行、标题正则全部失配 → 切块前统一归一化 `\n`。
7. **消息双写与状态机**：拒绝/取消路径保证"用户消息+已生成部分内容"都落库（buildCompletionPayloadOnCancel），否则前端刷新丢半截对话。
8. **取消的跨实例问题**：用户 stop 请求可能落在别的节点 → Redis bucket + RTopic 广播 + **本机也走 listener 统一处理**（避免重复 cancelLocal 双触发）；发布前先 set 标记，晚到的 register 回查 Redis 兜底（register 时发现已取消 → 立刻发 CANCEL+DONE）。
9. **知识库调度的锁丢失**：持有锁期间心跳续租失败/租约过期被他人抢走 → 所有写状态前 must `shouldAbortForLeaseLoss` 校验令牌，否则会把别人的执行状态改脏。
10. **MQ 消费者无用户上下文**：异步线程里 Sa-Token 上下文已丢 → 消费者手动 `UserContext.set(operator)` + finally clear；由此引出所有 do* 方法都不依赖 Web 上下文（用 TTL 的 UserContext）。

---

## 8. 系统局限性 & 可优化方向

> 已实现 = 当前代码存在并可讲；仅设想 = 面试时报"roadmap"，不要说成已上线。

### 8.1 局限与改进

| # | 局限（现实现） | 影响 | 改进方向 | 状态 |
|---|---|---|---|---|
| 1 | ModelHealthStore 是单 JVM 内存 Map | 多实例不共享熔断状态，恢复重复探测 | 熔断状态入 Redis（或直接用 Resilience4j 集群 pubsub） | 仅设想 |
| 2 | 意图树每次请求 read Redis + Jackson 反序列化整棵 | TTFT 里浪费 10–50ms | 进程内 Caffeine 短 TTL + 版本号失效（channel 变更广播）| 仅设想 |
| 3 | 限流轮询固定 200ms + PubSub 兜底 | 长队列场景感知延迟 | permit 释放源驱动（已有 PubSub，可把轮询间隔指数退避） | 已实现PubSub、轮询兜底仍在 |
| 4 | 后处理器仅 2 个（去重/Rerank） | 无关键词/分数域值过滤（Milvus 检索里还有 TODO：低分丢弃阈值 0.65、高分时扩大 1.5 倍检索） | 增 ScoreThreshold / VersionFilter，实现 SearchResultPostProcessor 即插即用 | TODO 在代码中 |
| 5 | 无 BM25/ES 混合检索通道 | 精确词（编号、型号）匹配弱 | SearchChannelType.KEYWORD_ES 预留位落一个 ES 通道实现 | 仅设想 |
| 6 | Enhancer/Enricher 每段一次 LLM 调用 | 入库慢、成本高 | 批量合并请求（一次多 chunk 摘要）；并发化 | 现为串行 for 循环 |
| 7 | Trace 节点逐条同步写 PG | 节点多时写放大 | 异步 buffer 批量落库 + 采样开关 | 仅设想 |
| 8 | 摘要 LLM 失败会丢弃本次摘要（catch 后仅日志） | 已窗口过界部分下次仍会带上 | 重试/降级用旧摘要拼接 | 已实现幂等重试语义（下一次 append 再试） |
| 9 | 单机演示级数据（无多租户隔离/权限过滤检索） | 企业级差 RBAC over retrieval | intent 表加 owner/scope，检索前按用户过滤 | 仅设想 |
| 10 | eval/ 仅骨架（EvalController/Properties/Response） | 无自动化检索评估闭环 | golden set 回归评测报表 | 仅设想 |
| 11 | ChatEntry 直通分支线程池拒绝需转 reject —— 处理了；但 fair limiter 的 maxWait 与 SSE 前端无等待 UI 联动（SSE 只在获得后推内容） | 用户等待体验 | SSE 推"排队第 N 位"进展事件（类型已预留 REJECT/META，缺 QUEUE_PROGRESS） | 仅设想 |
| 12 | 改写/意图/摘要/标题四类小 LLM 调用串在前置链路 | TTFT 抬升 | 并行（意图与改写无依赖时）；小模型专用低延迟通道 | 仅设想 |

### 8.2 已实现但面试可称"生产特性"的能力清单
限流（两级：聊天公平队列 + 上传信号量）、熔断（三态+候选链）、容错（首包探测/通道降级/后处理器跳过）、可观测（双 TTFT + 层级栈）、幂等（用户级 SpEL + 消息去重语义）、事务一致性（MQ 事务消息 + 编程式事务统一写）、异步化（MQ 消费、异步摘要、流式线程池）、安全（Sa-Token + 数据权限 StpInterface + SSE 响应不泄内部结构提示词约束）。

---

## 9. 面试高频问题 + 参考答案

### 9.1 基础题

**Q1 介绍一下这个项目。**
见 §10 自我介绍稿。

**Q2 双路召回为什么这么设计？只做向量检索行不行？**
不行。单全局向量检索的问题是：多库场景下召回会被相近语义的其他库内容稀释，topK 全被无关库占用；单意图定向的问题是意图识别失败/低置信时一次召回都没有。所以做成"意图定向（精准路由到 collection）为主，全局向量为置信度驱动的兜底"，代价用条件触发控制（全局通道只在无意图/低置信/定向关闭时启用），收益是覆盖率+精准度同时保住。refactoring-summary.md 里就是这个演进结论。

**Q3 Milvus 和 pgvector 你们怎么选的？**
一个 `rag.vector.type` 配置项切换两套 `RetrieverService/VectorStoreService` 实现（@ConditionalOnProperty）。Milvus：数据量/专用引擎场景（HNSW, ef=128, COSINE，查询向量先 L2 归一化）；pgvector：`t_knowledge_vector` 单表 + metadata jsonb（gin 索引），collection 隔离放 metadata 字段，检索 `SET hnsw.ef_search=200` + `1-(embedding <=> vec)`。默认 milvus，当前线上配置 pg。

**Q4 TTL 是什么？为什么这里必须用？**
TransmittableThreadLocal：线程池场景下 ThreadLocal 的值随任务提交"传递到执行线程"（池化线程复用导致普通 ThreadLocal 串号；InheritableThreadLocal 只在 new Thread 时复制、且无法回收）。项目里 UserContext（用户）、RagTraceContext（traceId/taskId/节点栈深拷贝）全走 TTL，且线程池统一用 TtlExecutors 包装，保证 trace 层级在"子问题→通道→collection"三级嵌套并行下不失真。

**Q5 幂等怎么做的？**
`@IdempotentSubmit`：注解里 key 用 SpEL（如取 UserContext.getUserId()），无 key 时 `接口 path + userId + 参数 MD5`；Redisson tryLock 拿不到即判定重复提交直接抛业务异常；finally unlock。SSE 对话用它防一个人并发双请求把排队资源占两份。

**Q6 你们的提示词怎么管理？**
外置 `prompt/*.st`；单文件多 section（`--- section: name ---`）+ `{slot}` 占位符（PromptTemplateLoader 内存缓存）；意图节点支持三级覆盖（snippet=块内 `<rules>` / template=整系统提示词替换 / paramPrompt=MCP 提参专用）；场景四模板（kb/mcp/mixed/system）+ 12 个结构 section（context-format.st）。所有 LLM 文本输出走 LLMResponseCleaner 剥 markdown 围栏。

### 9.2 深挖原理题

**Q7 详细讲一下公平排队限流的完整算法。**（连环追问链：*为什么 ZSET？→ score 是什么？→ maxRank 怎么来？→ slack=16 干嘛的？→ entry TTL 为什么=等待+5s？→ PollNotifier 为什么要 CAS？→ permit 拿到了又还回去的路径有哪些？*）
答：按 §4.10 + §3.1.3 顺序讲：入队（entry 先、ZADD seq 作 score）→ 抢占（availablePermits → Lua 队头窗口 ZRANGE 0, maxRank+16-1；存活成员计数定位 liveRank；liveRank<maxRank 才 claim，返回原 score、ZREM+DEL 标记）→ semaphore.tryAcquire(0, lease) → grant/pending 三分支（重入队 / 释放换队）→ 轮询+广播双驱动唤醒 → 终态四选一互斥 CAS。追问点全部在源码注释里（"slack 用于僵尸密集时尽量推进存活条目至 maxRank 之内"、"TTL 在 maxWait 之上的额外缓冲，避免毫秒级时钟漂移导致存活条目被误判为僵尸"、"firing CAS + pendingNotifications 合并：连续多次通知只触发一次扫描"）。

**Q8 熔断器讲细一点 —— 半开状态怎么防止并发放行多个请求？**（追问：*为什么不用 synchronized？isUnavailable 和 allowCall 什么区别？恢复的时间参数怎么定？*）
ModelHealthStore 用 `ConcurrentHashMap.compute(id,...)` 原子转移：HALF_OPEN 状态用一个 `halfOpenInFlight` 布尔，allowCall 里看到 HALF_OPEN 且 inFlight 已置位 → 返回 false，**只在 inFlight 从 false→true 的那次才放行**；markSuccess/markFailure 同样 compute。之所以 compute 而不是 synchronized：想保持读路径无锁（isUnavailable 供 ModelSelector 在候选阶段过滤）。参数来自 yaml `ai.selection.failure-threshold=2 / open-duration-ms=30000`；首包探测兜 60s。

**Q9 首包探测怎么做到"切换模型用户无感"？**（追问：*探测期间的内容去哪了？失败后缓冲内容怎么处理？NO_CONTENT 为什么算失败？*）
ProbeStreamBridge 把真实 downstream 包装起来：首个 onContent/onThinking 到达时 complete(SUCCESS)；此前所有事件进 buffer；await 成功后 commit() 一次性按原顺序回放（lock + committed 标志保证不再缓冲）。TIMEOUT/ERROR → bridge 里的内容连同 handle.cancel() 丢弃，换下一候选 —— 前端只感知到"首字来得稍晚"，不会看到半截脏内容。onComplete 但从未 onContent（NO_CONTENT）判失败是防"空响应也当成功"。

**Q10 意图识别的 cap 策略为什么这么设计？**（追问：*为什么 MAX=3？如果 5 个子问题每个 2 个意图会怎样？*）
多子问题×多意图会发生 collection 检索笛卡尔放大（注释："防止拉取过多 Collection 导致性能问题"）。capTotalIntents 先给每个子问题保底它自己的最高分（保证每个问题都有路可走），再按全局分数从剩余候选里补足到 3。5×2=10>3 时会被裁成"5 个保底 + 全局前 2 高分补充"，重组成新的 SubQuestionIntent 列表。

**Q11 记忆摘要的水位线具体怎么工作？**（追问：*摘要更新到一半进程挂了？两条消息并发触发摘要？*）
t_conversation_summary 保存 `last_message_id`；下次摘要区间 = (上次 lastMessageId, 近窗口最早 user 消息 cutoffId]，若 afterId≥cutoffId 说明窗口界之前已消化，直接放弃。并发：RedissonRLock tryLock，抢不到的本次直接放弃（下一条消息 append 时还会再触发）—— 保证幂等；进程挂：区间没有落 summary，重跑天然涵盖（区间由 DB 状态推导，非内存）。

**Q12 结构感知分块"只在块边界切"为什么重要？**
分块是检索与生成的最小单元：从中间剖开一段代码块/表格/标题-首段会破坏语义并使检索片段不可用。所以先分段成 HEADING/CODE/ATOMIC/PARA 块（record start/end 下标，substring 恒等原文），再按 min/target/max 预算整块打包，超长段才在自身边界内二次拆；overlap 是把上一 chunk 的尾部子串复制到下一 chunk 头部（不是切在中间）。

**Q13 RocketMQ 事务消息在这个项目里解决什么问题？**
"DB 记 pending + 发消息"两步非原子：普通消息可能先发后回滚（消费者空跑）或后发宕机（永远收不到）。half 消息 + 本地事务（改状态）+ 回查 TransactionChecker 完成"DB 成功才可见"；消费端 executeChunk 幂等（doc 状态判重），重试由 status 语义保证。

**Q14 USER_TTFT 和 LLM_TTFT 有什么区别？为什么要分开存？**
LLM_TTFT = 首包探测那 60s 窗口内的模型表现（LlmFirstPacketProbe 节点）；USER_TTFT = pipeline 入口到推给用户第一个字，包含改写/意图/检索/多模型切换全部前置 —— 反映的是"用户体感"。分开放 t_rag_trace_node，把"模型慢"和"链路慢"拆开归因，这是排障时最常用的两个指标。

### 9.3 压力质疑题（诚实、可守）

**Q15 “README 说 8 个线程池/ProbeBufferingCallback/四类后处理器 —— 和代码对不上，项目真是你做的？”**
如实回答：宣传物料有历史/口径问题；以代码为准 —— 10 个线程池（多出 knowledgeChunk、memoryLoad 两个，对应知识库/记忆模块）；首包探测类实为 ProbeStreamBridge，行为与描述一致；后处理器接口 Javadoc 列了四种示例、实装去重+Rerank 两个（版本过滤/分数归一化属于预留）。能讲出每一处的取舍与竞态细节，本身就是真实性的证明。

**Q16 “熔断健康存在单机内存，多实例部署不就各熔各的？”**
是。当前限制：多实例各自探测各自恢复，重复放行探测请求的代价是每个实例最多 1 个半开请求/模型，可接受但不优雅；改进方向是状态外置 Redis + pubsub。主动说出比被问出来好。

**Q17 “每轮问答要打多少次 LLM？成本与时延怎么算？”**
前置小调用：改写 1 次(+温度0.1)、每子问题意图 1 次（并行）、澄清边界区间时 +1、MCP 每工具参数 1 次、标题（仅首条）、摘要（异步，不阻塞）；主生成 1 次（流式）。即典型 3~5 个小调用 + 1 大调用。TTFT 主要被前置链路吃掉 —— 这是我列的 roadmap（改写/意图并行化、小模型专用通道）。

**Q18 “为什么意图分类不用向量？为什么不用 function calling？”**
向量方案做过实验（test/VectorIntentClassifier，IntentNode.embedding 已标 @Deprecated），放弃原因 §6.1。function calling：项目走"意图 → 节点 mcp_tool_id → 参数提取"的确定性路由，可控性和 Trace 完整性好，工具范围被意图树显式管理；这不是能力不及，是场景取舍（内部知识库不需要开放式工具探索）。

**Q19 “如果流量涨 100 倍，哪里先挂？”**
排序：① 全局向量通道并行扫全 collection（collection 数×并发放大）→ 先把全局兜底从"扫库"改成"目标候选库采样/异步预索引检索别名"；② 单机线程池容量（SynchronousQueue 立即失败型在线程不够时拒绝）→ 有界扩容+按 pod 水平扩（限流本身分布式）；③ Trace 同步写库 → 批量/采样；④ 意图树反序列化 → 本地缓存。限流器天然横扩（Redis 状态），chatEntryExecutor 与 maxConcurrent 绑定需要随并发配置调整。

**Q20 “怎么防幻觉/提问安全？”**
三层：检索边界（prompt 的 "信息边界最高约束"，只答 `<documents>/<tool-data>` 可见内容、不足明说）；摘要不存答案（防止旧答案当事实）；SSE 结构不暴露（`<rules>` 不得复述；链接带敏感参数不输出完整 URL）；管理侧 Sa-Token 数据权限 + demo 模式 + eval 旁路幂等。

---

## 10. 项目自我介绍

### 10.1 一分钟版本

> 我做了一个企业级 RAG 智能问答平台 Ragent，Java 17 + Spring Boot 3，四个后端模块加 React 前端。核心链路是：用户提问先经过术语归一化和 LLM 改写拆分，再按树形意图把每个子问题路由到对应知识库做向量检索，全局向量通道做置信度驱动的兜底，多通道结果去重加 Rerank 后组装场景化提示词流式生成；非知识类问题通过 MCP 协议调用外部工具拿实时数据。工程上我重点做了三件事：一是基于 Redis 的分布式公平排队限流，ZSET 保序加 Lua 原子出队，解决了高并发下模型入口被打爆和进程崩溃占坑的问题；二是多模型供应商的路由容错，三态熔断加流式首包探测，首包前缓冲、超时无感切换，用户端感受不到模型故障；三是全链路 Trace，AOP 维护节点层级栈配合 TransmittableThreadLocal 跨线程池透传，能精确拿到每一轮问答的用户感知首包时延。数据侧用 PostgreSQL + pgvector，可一键切 Milvus，文档入库走 RocketMQ 事务消息异步分块，还有一套可视化编排的摄取流水线。

### 10.2 三分钟版本

> （在 1 分钟版骨架上展开，结构：背景 → 检索链路 → 三大工程亮点 → 技术栈收尾）
>
> 背景：企业知识散在 PDF、Word、飞书、远程 URL 里，用户口语提问和标准术语也对不上，所以我们把"查询理解"做成了流水线一等公民：先用数据库 + Redis 的术语映射做归一化（长词优先、按优先级排序），再让 LLM 做改写和多问句拆分——保留专有名词、去掉客套话、基于最近历史做指代消解，LLM 挂了就退化成规则按标点拆分，任何一步都有兜底。
>
> 检索是双通道的：树形意图（Domain/Category/Topic 三级，存 PG 缓存 Redis）把叶子节点渲染给 LLM 打分，识别出 KB 意图后按节点去对应 collection 定向检索；当意图置信度低于阈值或者干脆没有识别出意图时，启动全局向量通道对全部知识库兜底，两路结果按通道优先级去重、再过 Rerank 截断。意图层面还有个我很喜欢的设计：当同一个主题词命中多个系统时，比如"数据安全"在 OA 和保险系统都有，我们不是硬答，而是先用分数比值的规则做初判，边界区间再花一次小 LLM 调用确认，确认歧义就直接向用户抛选项让他回复数字——宁可多问一句，不答错方向。纯工具类的意图走 MCP：架构里有个独立的 mcp-server 进程，用官方 SDK 的 Streamable HTTP 传输，客户端启动时 listTools 自动注册，调工具前由 LLM 按工具的 JSON Schema 提取参数，工具结果和知识库文档分别用 tagged 容器拼进同一个提示词。
>
> 工程上讲三个最有含金量的点。第一，分布式公平排队限流：入口是 Redis ZSET 按序排队、RPermitExpirableSemaphore 控并发——选它是因为许可带租约，进程崩溃 30 秒后自动回收，不会死锁占坑；出队用一段 Lua 原子判断"这个请求是否在存活队头窗口内"，同时把过期 entry 当僵尸清掉；唤醒靠 Pub/Sub 广播，加了一个 CAS 加计数器合并通知，防止广播风暴。429 也走完整会话语义：用户问题、拒绝回复都落库，前端历史可见。第二，模型容错：候选链按优先级排，熔断器三态，半开状态只放行一个探测请求；流式调用外面套了一层桥接装饰器，把首包之前的事件全部缓冲，阻塞等第一个字最多 60 秒——成功就顺序回放缓冲，失败静默丢弃切下一个供应商，用户完全无感。第三，可观测性：Trace 用 AOP 维护节点层级栈，上下文全走 TransmittableThreadLocal，其中节点栈要深拷贝，否则并发子任务会共享一个栈互相串父节点；我们分别记录模型首包时延和用户感知首包时延，一个归因模型慢，一个归因链路慢。
>
> 技术栈收尾：Spring Boot 3.5、MyBatis-Plus、PostgreSQL + pgvector（可切 Milvus，一套接口两实现）、Redisson、RocketMQ 事务消息做文档异步分块、Tika 多格式解析、Sa-Token 认证、React 18 管理端。数据库 21 张表，摄取侧还有一套节点编排流水线——链式执行、环检测、条件跳转、每节点独立日志，文档从解析、分块、LLM 增强、向量化到入库全程可定位到节点级。

---

## 附：速查卡（考前 10 分钟看这个）

| 主题 | 关键词/数字 |
|---|---|
| 线程池 | 10 个：chatEntry(=maxConcurrent,Sync,Abort) / ragContext / ragRetrieval(4C,Sync) / innerRetrieval(队列100) / intentClassify / mcpBatch / memoryLoad / memorySummary / modelStream(队列200,Abort) / knowledgeChunk；全 TtlExecutors |
| 限流 | ZSET(队头窗口 maxRank+slack16) + RPermitExpirableSemaphore(lease 30s) + RTopic(合并通知) + Lua(queue_claim_atomic.lua)；maxConcurrent 10、maxWait 15s、poll 200ms；Ticket 四态 CAS |
| 熔断 | CLOSED/OPEN/HALF_OPEN；failure 2 次 → OPEN 30s → HALF_OPEN 单探测（halfOpenInFlight）；候选阶段过滤 isUnavailable |
| 首包探测 | ProbeStreamBridge 缓冲；await 60s；SUCCESS/ERROR/TIMEOUT/NO_CONTENT；LlmFirstPacketProbe 独立 bean（AOP self-call） |
| 意图 | INTENT_MIN_SCORE 0.35；MAX_INTENT_COUNT 3（保底策略）；树缓存 ragent:intent:tree TTL 7d；三级 DOMAIN/CATEGORY/TOPIC；KB/SYSTEM/MCP |
| 检索 | 通道优先级 Intent(1)>ES(2)>Global(3)；global 置信阈值 0.6；定向 minIntentScore 0.4；topK 默认 10，multiplier 定向×2/全局×3 |
| 记忆 | 滑窗 historyKeepTurns×2 条；摘要启动 5 轮、≤200 字、水位线 lastMessageId、RLock；摘要禁存答案；标题 30 字 |
| 温度 | 改写/意图/提参 0.1；生成 KB 0.0，MCP 0.3；标题 0.7 |
| 向量 | 1536 维 COSINE；Milvus ef=128；PG hnsw ef_search=200；查询前 L2 归一化 |
| 分块 | 固定 512/overlap128；结构感知 HEADING/CODE/ATOMIC/PARA；content>65535 截断 |
| Trace | USER_TTFT(链路) + LLM_TTFT(模型)；RagTraceContext 节点栈深拷贝；RagTraceAspect Order=HIGHEST+10 |
| 消费/取消 | MQ topic knowledge-document-chunk_topic（事务消息+回查）；cancel bucket TTL 30min + RTopic：ragent:stream:cancel |
| 上传 | UploadRateLimitFilter 在 multipart 前限流（max 10、wait 5s、lease 300s），满 429 |
| 调度 | 扫描 10s、租约 lock_until 900s+心跳、ETag/If-Modified-Since、content hash 不变则跳过；60s Job 恢复 RUNNING 超时 |

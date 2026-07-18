# RMDB 通用自适应 JIT 与 StatementTemplate Cache 实施计划（基于 AsmJit）

> 文档状态：实施前设计计划；JIT/StatementTemplate 生产源码尚未实施，本文只定义落地阶段与门禁。
>
> 调研基线：RMDB `9da2af3`；`reference/postgres` `bb7ded1eebed708865d9bb0a3513c7ed3afe7065`；
> `deps/asmjit` `0bd5787b54b575ed94bf32ac452153b34385c514`（AsmJit 1.21.0）。

## 1. 名称、范围与结论

AsmJit库在当前仓库中实际为 `deps/asmjit`。本文以下均按当前已经 clone 到该目录的
AsmJit 1.21.0 设计；实现时直接把这份源码作为 CMake 子项目使用，最终提交前再由仓库跟踪该源码目录，
不重新克隆或切换到其他 AsmJit 版本。

最终提交指该计划的设计方案全部落地到代码中，并最后由用户确认之后进行的提交。

本计划的目标不是识别某几条 TPCC SQL 后生成专用代码，而是分两步消除当前 flamegraph 中的四个热点：

1. **先优化 Executor：** 从优化后的语义计划生成与常量值无关的 JIT 指纹，把每次变化的字面量绑定到参数块；对任意满足支持条件的计划累计热度，将谓词、投影、连接条件、更新算术和聚合转移等纯计算降低为类型化 JIT IR，再通过 AsmJit 生成并复用机器码。
2. **再优化 Parser/Analyzer/Planner：** 使用现有 Lexer 生成拥有自身存储的规范化 token/参数流，在 Parser 之前查询 `StatementTemplateCache`；最终热命中只绑定当前 literal、实例化执行级可变状态并取得已缓存的 JIT Kernel，不再调用 Parser、Analyzer 或 Planner。
3. `StatementTemplateCache` 与 `JitCodeCache` 分层：前者缓存不可变的已解析/已分析/物理执行蓝图，后者缓存按语义 IR 与布局指纹生成的机器码。两者可独立命中、淘汰和回退，不能以 SQL 原文或 literal 值作为 key。
4. 未达到阈值、模板 miss、DDL/配置失效、编译失败、架构不支持或语句暂不支持时透明使用现有完整流水线或解释 Executor。
5. MVCC 可见性、SSI 读跟踪、锁、索引游标、WAL、事务状态和输出格式继续由现有 C++ 路径负责，不进入生成代码或共享模板的可变状态。

推荐把一个热点 SQL 的纯计算编译成一组可复用的 `CompiledStatement` Kernel，并把其前端/物理结构保存为一个不可变 `StatementTemplate`，而不是缓存整个 Executor、mutable `Plan` 或某段 SQL 文本。首批 Kernel 覆盖谓词和更新表达式，随后扩展到投影、聚合及安全的算子融合；JIT 稳定后再逐级打开前端 bypass。这一边界既能覆盖 TPCC 高频语句，也能自然服务于普通 OLTP/OLAP SQL。

需要提前明确一个性能事实：TPCC 是大量短事务，索引、锁、MVCC、WAL、网络和前端流水线开销都可能高于表达式计算。表达式 JIT 只直接处理 Executor 热点；`StatementTemplateCache` 才负责让 Parser/Analyzer/Planner 在高命中率稳态 flamegraph 中接近消失。这里的“消失”只指有效模板热命中：残余占比约由 miss/失效率、轻量 Lexer 归一化、参数绑定和蓝图实例化成本决定，不能承诺绝对为零。若最终瓶颈转移到锁或存储层，应如实报告，不能用 TPCC 特例硬编码制造结果。

## 2. 成功标准与非目标

### 2.1 功能成功标准

- 热点识别只依赖规范化计划结构、类型、列布局和运行统计，不依赖 SQL 原文、TPCC 表名或事务名。
- 相同结构、不同 `INT`/`FLOAT`/`CHAR`/`DATETIME` 参数的语句命中同一代码条目；结构、类型、列布局或物理计划不同的语句不能错误复用。
- 相同 token 结构、不同 literal 的语句命中同一 `StatementTemplate`；完整模板热命中时 Parser、Analyzer、Planner 的入口调用计数均为零，只运行 Lexer 归一化、参数绑定、执行蓝图实例化和 Executor。
- `RMDB_JIT=off` 与 `RMDB_JIT=force` 对所有支持 SQL 产生完全相同的结果、错误类型、事务效果和 `output.txt` 字节流。
- `RMDB_STATEMENT_CACHE=off|shadow|parser|analyzer|full` 各模式产生完全相同的结果、错误文本、事务效果和 `output.txt` 字节流；任一模板校验或内部绑定失败都能回退完整流水线。
- JIT 编译或代码分配失败只降低性能，不导致用户语句失败。
- DDL、数据库重开、planner knob 变化、缓存淘汰、并发建模和并发编译均不能造成旧蓝图/旧代码调用、悬空指针或 use-after-free。
- 未支持的平台或表达式自动回退；支持范围可以逐阶段扩展而不改变 Executor 语义。

### 2.2 性能目标（作为验收门槛，不作为未经测量的承诺）

- 微基准中，整数/浮点多条件谓词相对当前 `conditions_match()` 至少降低 40% CPU cycles；包含投影或更新算术的组合 Kernel 至少降低 30%。
- TPCC 固定数据集、Release 构建、同一并发度下，`auto` 模式五轮中位 tpmC 的目标提升为至少 20%，同时事务 abort rate 不增加 1 个百分点、各事务 p95 延迟不恶化 5% 以上。
- 测量阶段 TPCC 热点语句结构的模板缓存和代码缓存命中率分别达到 95% 以上，建模/编译在 warmup 内完成，测量期不出现持续建模或编译风暴。
- 对 Parser、Analyzer、Planner、Executor 分别报告绝对 cycles/transaction 和 flamegraph 占比；`full` 模式稳态热命中样本中前三者不得出现，整体样本中的残余占比必须与模板 miss/失效次数相符。
- 默认代码缓存受内存上限约束，稳定运行时 RX 代码占用不超过配置值，初始建议为 16 MiB。
- 若 Kernel 微基准达标而整体 tpmC 提升不足 10%，必须重新 profile；只有 profile 证明 Executor 计算仍是主要瓶颈时才继续做融合，否则结束 JIT 扩面并记录瓶颈。

### 2.3 非目标

- 不新增 TPCC 专用语法、函数、表识别或固定 SQL 模板。
- 不把 B+ 树遍历、BufferPool、锁管理、MVCC 版本链、WAL 或事务提交逻辑翻译成机器码。
- 第一版不做持久化机器码缓存。生成代码含进程内 helper 地址和 CPU 特征，只在当前 server 进程中有效。
- 不新增 SQL `PREPARE/EXECUTE` 协议；JIT 参数化最初发生在计划之后，模板缓存参数化最终前移到 Lexer 之后，二者都对客户端透明。
- 不引入 LLVM，也不照搬 PostgreSQL 的全部 JIT 基础设施。
- 不修改普通查询、错误、`EXPLAIN ANALYZE` 或 `output.txt` 的可见格式。若以后希望在 SQL 输出中展示 JIT 统计，必须先单独确认输出格式变更。

## 3. 当前代码事实与 JIT 落点

| 当前事实 | 源码证据 | 对设计的约束或机会 |
| --- | --- | --- |
| 每条请求依次经过 parse、analyze、plan、Portal、Executor | `src/rmdb.cpp:152-181` | 仅在 Plan 后接 JIT 不能消除前三个热点；最终模板查询必须前移到 `parse_sql()` 之前，命中后再沿用现有事务判定和 Portal/Executor 路径。 |
| Lexer 已区分 identifier、运算符和四类 literal，数值也在 Lexer 中解析 | `src/parser/lexer.h:50-158`、`src/parser/lexer.cpp:217-245` | 直接复用 Lexer 构建 token shape，不用 regex 改写 SQL；缓存 token 不能保留指向请求缓冲区的 `string_view`，必须拥有 identifier/string 字节。 |
| Parser 直接拥有 Lexer，公开 API 只有 `parse_sql(string_view)`；一元负号由独立 `MINUS` token 与 Parser 联合处理 | `src/parser/parser.h:20-27`、`src/parser/parser.cpp:34-113, 419-486` | Phase 7 先抽出可复用 owned token stream；参数描述必须保留负号上下文和现有整数边界错误，不能把每个 `MINUS + number` 盲目合并。 |
| AST 只有表达式级 clone helper，没有完整 statement clone | `src/parser/ast.h:277-340` | Parser bypass 不能假设现有 AST 可安全复制；需补齐不可变 statement skeleton/显式实例化器，并用所有权测试约束。 |
| Analyzer 解析别名/列、校验语义，并由 `Query::parse` 持有 AST | `src/analyze/analyze.h:26-61`、`src/analyze/analyze.cpp:108-208` | Analyzer bypass 必须缓存已解析的不可变语义描述及参数转换 recipe，不能共享/浅拷贝当前 `unique_ptr` 对象图。 |
| `Portal::convert_plan_executor()` 按 `PlanTag` 递归构造执行树 | `src/portal.h:745-825` | 可在一个位置把编译句柄注入节点；完整模板命中时仍在这里为每次执行构造独立 Executor 状态。 |
| `AbstractExecutor` 已缓存列偏移，并由 `conditions_match()` 逐条件解释比较 | `src/execution/executor_abstract.h:146-260` | `ConditionAddress` 基本就是谓词 IR 的输入；应保留解释实现作为 oracle 和回退。 |
| SeqScan 和 IndexScan 都在可见性检查后调用同一条件循环 | `src/execution/executor_seq_scan.h:96-139`、`src/execution/executor_index_scan.h:548-565` | 一个通用单 tuple Predicate Kernel 可同时服务顺序扫描和索引残余过滤，MVCC/SSI 留在 Kernel 外。 |
| Filter 使用借用的 `TupleView`，避免不必要复制 | `src/execution/executor_filter.h:45-79` | JIT ABI 可直接接收 `const char *` 和 tuple 长度，不需要生成代码理解 `RmRecord` 所有权。 |
| Projection 已把投影解析为源/目标固定偏移的 memcpy 循环 | `src/execution/executor_projection.h:31-44` | 可生成固定偏移 load/store，并合并相邻 copy span。 |
| NLJ 已把条件预编译为左右 operand、offset、type | `src/execution/executor_nestedloop_join.h:31-48, 136-213` | Join Kernel 只需接收左右 tuple 指针和参数块，不必先物化拼接 tuple。 |
| Update 对 `SetClause` 做类型转换和加、减、乘、除运算 | `src/execution/executor_update.h:252-356` | 可生成 Update Kernel，但除零、类型错误等必须先返回状态，再由 C++ wrapper 抛出现有异常。 |
| Aggregate 的热点是 read cell、transition、HAVING 和结果 materialize | `src/execution/executor_aggregate.h:340-375, 493-595` | 先 JIT 无分组数值 transition，再扩展到 selection+transition；哈希表和字符串所有权继续留在 C++。 |
| SELECT 已有容量 256 的参数值无关物理计划模板缓存，但查询发生在 Analyzer 之后，命中仍实例化新 `Plan`；DML 仍按当前 literal/Query 规划 | `src/optimizer/planner.h:45-79`、`src/optimizer/planner.cpp:1088-1158, 1396-1451` | 可复用 canonical shape 和蓝图实例化思路，但它不能直接实现前三段 bypass；最终应并入一个覆盖 SELECT/DML 的 `StatementTemplateCache`，不能长期叠加三个阶段缓存。 |
| `SmManager` 已提供原子 `catalog_generation_`，并在 open/close/DDL 后递增 | `src/system/sm_manager.h:76-79, 124-125`、`src/system/sm_manager.cpp:212, 251, 506, 526, 588, 611` | 直接作为模板和代码缓存的 schema epoch；初期无需新造失效通知系统。 |
| 每连接一个 detached 线程，共享全局 Planner/Portal/Manager | `src/rmdb.cpp:42-63, 295-314` | 热点统计和两个缓存都必须并发安全；关闭时必须先停止 lookup/publish/编译并等待在途执行释放模板和代码。 |
| CMake 当前为 3.16，`deps` 只加入 googletest | `CMakeLists.txt:1-20`、`deps/CMakeLists.txt:1` | AsmJit 子项目要求 CMake 3.24，实施前需统一最低版本并显式链接 `asmjit::asmjit`。 |

## 4. TPCC 热点为何能由通用机制覆盖

### 4.1 静态工作负载分析

事务选择比例来自 `benchmark/tpcc/go/cmd/tpcc-go/main.go:724-739`：

| 事务 | 比例 | 结构性热点 |
| --- | ---: | --- |
| `new_order` | 45% | 每事务有 5 个固定语句，随后循环 5-15 次（均值 10）；循环内有 item 点查、两次 stock 更新、stock 点查和 order_line 插入。每 100 个事务中，每个循环 SQL 结构约执行 450 次。 |
| `payment` | 43% | 7 个固定结构，集中在复合整数谓词、固定投影及多列自增/自减 Update。 |
| `order_status` | 4% | 复合谓词、字符串等值、COUNT、ORDER BY 和 order_line 范围读取。 |
| `delivery` | 4% | 通常按每仓库 10 个 district 循环，包含 MIN/SUM、复合谓词、Delete 和 Update；每个内部结构约执行 40 次/100 事务。 |
| `stock_level` | 4% | 先做 order_line 范围读取，再按返回 item 反复执行 stock COUNT；内层次数随数据变化，天然适合运行时热度统计。 |

高频 SQL 的具体位置为 `main.go:471-715`。它们共同使用的不是某个 SQL 字符串，而是以下通用计算原语：

- 一个或多个固定偏移列与运行时参数的 `=`, `<>`, `<`, `>`, `<=`, `>=` 比较及 AND 短路。
- 多列定长投影和 tuple copy。
- `column = column op parameter` 形式的加、减、乘、除，以及跨 `INT/FLOAT` 的当前既有转换规则。
- `COUNT(*)`、`MIN`、`SUM` 及过滤后聚合 transition。
- 左右 tuple 列之间的 Join 比较。

所以实现应针对这些 IR 操作，而不是针对 `stock`、`district`、`order_line` 等名字。相同机制应能编译用户创建的任意布局相容表和相同语义算子。

### 4.2 优先级

1. **P0：Predicate Kernel。** 覆盖 SeqScan、IndexScan 残余谓词、Filter 和 NLJ 条件，复用面最大。
2. **P0：Update Kernel。** TPCC 写事务密集，自增/自减表达式重复率极高。
3. **P1：Projection Kernel。** 点查输出列多，固定偏移 copy 可与过滤融合。
4. **P1：Aggregate Transition Kernel。** 优先 `COUNT(*)`、数值 `SUM/MIN/MAX/AVG`，覆盖 delivery/stock_level。
5. **P2：安全的 tuple pipeline 融合。** 把 `filter + projection` 或 `filter + aggregate transition` 合成一次 Kernel 调用，减少虚函数和中间 tuple 开销。
6. **按 profile 决定：Insert tuple materialize。** Insert 的锁、索引、日志通常占主导，不能仅因 TPCC 中频繁出现就盲目 JIT。

## 5. PostgreSQL 参考实现的采用与调整

本计划参考 `reference/postgres/src/backend/jit/`，但根据 RMDB 的短 OLTP 工作负载做必要调整。

| PostgreSQL 机制 | 参考位置 | RMDB 采用方式 |
| --- | --- | --- |
| provider-independent `compile_expr` 回调，可返回 false | `reference/postgres/src/include/jit/jit.h:65-102`、`reference/postgres/src/backend/jit/jit.c:146-179` | 定义内部 `JitCompiler` 接口；所有 unsupported/error 都返回结果对象，Executor 永远保留解释回调。第一版无需动态插件系统。 |
| 以 `ExprState` step program 为统一解释/JIT 输入 | `reference/postgres/src/backend/executor/execExprInterp.c:248-457` | 建立小型类型化 `JitProgram`，同一 IR 同时有 C++ 解释器和 AsmJit backend，避免两套语义手写分叉。 |
| 按计划 cost 决定是否 JIT | `reference/postgres/src/backend/optimizer/plan/planner.c:698-721`、`reference/postgres/doc/src/sgml/jit.sgml:95-130` | RMDB TPCC 是大量短语句，改为跨执行累计的 observed hotness + break-even；不能为每次短语句同步编译。 |
| query 级 JIT context 统一管理代码生命周期 | `reference/postgres/src/backend/jit/README:89-124` | RMDB 代码要跨 query 复用，因此使用 process 级 `JitManager`、cache entry 级代码所有权和 execution 级参数块。 |
| 首次执行时再 materialize 代码 | `reference/postgres/src/backend/jit/llvm/llvmjit_expr.c:2953-3004` | 热点达到阈值后异步生成并发布；当前请求继续解释执行，后续请求无阻塞命中。 |
| 编译函数不应捕获 per-execution 指针，否则无法缓存 | `reference/postgres/src/backend/jit/README:222-239` | 这是本设计的硬约束：代码只固化 offset/type/op，所有 Value、tuple、输出和 aggregate state 通过调用帧传入。 |
| JIT 统计记录生成、优化、发射时间 | `reference/postgres/src/include/jit/jit.h:27-46`、`reference/postgres/src/backend/commands/explain.c:909-995` | 记录 observe/queue/compile/code bytes/hit/fallback 等指标，但初期只写内部 log/benchmark 数据，保持 SQL 输出不变。 |

PostgreSQL 的 cost 策略适合单次长查询；RMDB 的关键差异是让同一结构在成百上千次短执行之间摊薄编译成本。采用参数块也主动解决了 PostgreSQL README 指出的代码缓存障碍。还要明确边界：参考源码中的 PostgreSQL JIT 从已规划表达式/tuple deform 开始工作，并不会让 Parser、Analyzer 或 Planner 消失；本文的 `StatementTemplateCache` 是 JIT 之后单独实施的 RMDB 前端缓存层，不能把它误称为 PostgreSQL JIT 自带能力。

## 6. 总体架构

```text
SQL text
   |
Lexer -> owned normalized token/parameter stream -> StatementShapeKey
   |                                                   |
   |                                      StatementTemplateCache lookup
   |                                                   |
   |                    +------------------------------+---------------------------+
   |                    | miss / disabled / invalid                                | hit
   |                    v                                                          v
   +------------> Parser -> Analyzer -> Planner                  lexical bind + statement kind
                         |                                                          |
                         v                                                          v
               build/publish immutable StatementTemplate       preserve statement-kind/transaction decision
                         |                                                          |
                         |                                                          v
                         |                                        semantic bind + select bypass stage
                         |                                                          |
                         +------------------------------+---------------------------+
                                                        |
                                                        v
                                      instantiate execution-local state/Executors
                                                        |
                                    JIT descriptor -> JitCodeCache lookup
                                                        |
                                     +------------------+------------------+
                                     |                                     |
                              miss / unsupported                      code cache hit
                                     |                                     |
                           interpreted pure compute              generated tuple Kernel
                                     +------------------+------------------+
                                                        |
                                      existing MVCC/locks/WAL/output path
```

在最终 `full` 热命中路径中，图左侧的 Parser、Analyzer、Planner 分支不会被调用；Lexer 仍必须执行，因为客户端发送的仍是普通 SQL 文本，系统需要验证词法并提取本次参数。

### 6.1 四类生命周期

- **进程级：** `StatementTemplateCache`、`JitManager`、AsmJit `JitRuntime`、编译线程、全局容量限制和统计。
- **模板条目级：** owned canonical token shape、参数 recipe、schema/planner epoch、不可变语义/物理蓝图、输出元数据和 JIT descriptor；由 `shared_ptr<const StatementTemplate>` 管理。
- **代码条目级：** canonical IR、ABI/CPU tier、机器码地址、code size、状态和最后访问时间；由 `shared_ptr<const JitCode>` 管理。
- **语句执行级：** 当前 literal 的 owned binding、`JitParamBlock`、fresh Executor/iterator/aggregate state、tuple 指针、输出缓冲、事务引用和本次 metrics；执行结束立即销毁。

两个缓存都不得持有 `Context*`、`Transaction*`、mutable `Plan*`、`Executor*`、本次 `Value*`、`RmRecord*` 或请求缓冲区的 `std::string_view/data()`。在途执行分别持有模板/代码条目的强引用；LRU 淘汰只从 map 中移除，最后一个代码调用释放引用后才执行 `JitRuntime::release()`。

### 6.2 编译粒度

`CompiledStatement` 是 `StatementTemplate` 引用的 Kernel descriptor bundle，而不是一段硬编码 SQL：

- 一个或多个 `PredicateKernel`：单 tuple 或左右 tuple 条件。
- 可选 `ProjectionKernel`：固定 offset 的列复制/常量写入。
- 可选 `UpdateKernel`：从 old tuple 和参数计算 new tuple。
- 可选 `AggregateKernel`：更新一个 POD transition state。
- 后期可选 `FusedTupleKernel`：把相邻纯计算步骤合成一次调用。

Executor 控制流、游标和副作用保持现状。这样即使某个 Kernel unsupported，同一 statement 的其他 Kernel 仍可编译，且可以逐节点回退；代码条目被淘汰时，模板也仍可绕过前端并使用解释 Executor。

## 7. 双层指纹与自动参数化

### 7.1 两个 key 的职责

| Key | 生成时点 | 规范内容 | 用途 |
| --- | --- | --- | --- |
| `StatementShapeKey` | Lexer 产出完整 owned token stream 后、Parser 之前 | token 类型/顺序、按现有标识符语义保留的 owned identifier、literal 参数类型、运算符/标点、一元负号结构、template format version、catalog generation、planner knob epoch | 查询/创建 `StatementTemplate`，决定可绕过到哪一层。 |
| `JitCodeKey` | Analyzer/Planner 产出类型与布局确定的蓝图后，或从完整模板直接取得 | canonical JIT IR、tuple/column/state layout、JIT IR/ABI version、helper version、CPU feature tier、影响 codegen 的开关 | 查询/创建机器码；不关心 SQL 的空白、别名拼写或 literal 值。 |

不能用同一个 key 兼任两层缓存：同一 lexical shape 在 DDL/knob 后可能得到不同物理布局；不同 lexical shape 也可能降低成同一 Kernel IR。模板命中不要求机器码仍在，代码命中也不要求本次语句来自模板命中。

### 7.2 Lexer 归一化与 `StatementShapeKey`

直接复用当前 Lexer，不对 SQL 原文做 regex/字符串替换：

1. 扫描到 EOF 后才允许查缓存；词法错误直接沿用现有错误路径，不能以部分 key 命中。
2. keyword、运算符、标点和 identifier 保留 token 类型与顺序；identifier/string 内容复制到 owned arena。只有已经由测试证明与当前语义等价时才做大小写规范化。
3. `VALUE_INT/FLOAT/STRING/BOOL` 在 canonical stream 中替换为带 lexical type 的 parameter marker，本次原始/解析后值放入 `LexicalParamBlock`；literal 值、空白、源码位置不进入 key。
4. `MINUS` 仍是独立结构 token。shadow 阶段根据 Parser 结果生成/核对“负数字面量”参数 recipe，完整上线前必须覆盖 `INT_MIN`、越界和算术减号，禁止仅凭相邻 token 猜语义。
5. key 由 canonical bytes 加 `catalog_generation`、template format version 和所有影响语义/物理计划的 planner knob epoch 组成。

原始 SQL 文本不能作为 key：TPCC 每次把 literal 插入 SQL，raw-text cache 会按值膨胀，无法命中热点结构。

### 7.3 `JitCodeKey` 必须包含

- `JIT_IR_VERSION`、JIT ABI 版本、host architecture、目标 CPU feature tier。
- `catalog_generation`；计划节点类型及 child 顺序。
- 表身份、tuple 总长度；所有使用列的 table/column 身份、type、len、offset。
- 条件运算符、operand 来源（tuple 0/tuple 1/parameter）、参数类型和 string 最大长度。
- 投影 source/destination offset 和 copy length。
- Update 的目标类型、源类型、算术运算及转换模式。
- Aggregate 类型、输入布局、state 布局和 HAVING 结构。
- 会影响生成代码的 planner knob 或 JIT feature flag。

字面量数值、事务 ID、snapshot timestamp、RID、指针和本次输出名称不得进入 `JitCodeKey`。`LIMIT`、排序方式等虽不一定进入 Kernel，也必须保存在 `StatementTemplate` 的物理蓝图中；只有影响 Kernel 时才进入代码 key。

### 7.4 指纹编码与碰撞处理

- 复用 `planner.cpp` 当前 length-prefixed shape 序列化思想，但提取成不依赖 Planner 私有类型的 canonical writer。
- 不使用实现相关且进程间不稳定的 `std::hash` 作为唯一身份。先生成 canonical bytes，再计算稳定 128-bit digest；cache entry 同时保存 bytes。
- 命中时先比较 digest，再比较 canonical bytes，消除哈希碰撞导致错误代码复用的可能。
- 条件顺序必须与优化后的执行顺序一致；若为提高复用率排序，只能在已经证明 AND 条件无副作用且错误顺序不影响可见语义时进行。

### 7.5 两级参数绑定

`LexicalParamBlock` 拥有当前请求的原始/解析后 literal；模板中的 parameter recipe 把这些 slot 转换为本次 `BoundStatement` 所需的 `Value`、condition、set-clause、insert tuple 和 LIMIT 等字段。随后 JIT binding map 再生成只使用 POD 的 `JitParamBlock`，例如 type tag、`int32_t`、`double`、`const char* + uint32_t length`。字符串 slot 指向本次执行拥有的存储，机器码调用必须同步完成，任何缓存都不保存该地址。

绑定阶段执行以下校验：

- lexical 参数数量、token type、符号上下文和 template recipe 必须一致；JIT 参数数量和类型必须与 IR 完全一致。
- 整数范围、负号、字符串长度、目标列最大长度、cast 和 DATETIME 规则必须沿用 Parser/Analyzer/`Value::init_raw()` 的当前语义及错误文本。
- 不允许隐式产生 IR 未声明的类型转换。
- 用户值本身非法时，由 binder 产生与旧流水线相同的用户错误；template version/type/recipe 等内部不一致时熔断该 entry 并回退完整流水线，不能把内部错误暴露为新的 SQL 行为。

## 8. StatementTemplate Cache 设计

### 8.1 单一生产表示，而不是三套永久缓存

最终只维护一个 `StatementTemplateCache`。每个 immutable `StatementTemplate` 带明确 readiness，并按阶段逐步具备以下内容：

- canonical `StatementShapeKey` bytes/digest、statement kind、template format version、catalog/planner epoch 和依赖表集合。
- parameter descriptors：token slot、lexical type、负号/显示文本规则、目标语义类型、长度/cast/range 校验及绑定目的地。
- parsed statement skeleton，用于 Phase 8 实例化 fresh AST；不能共享当前 `unique_ptr` 对象图。
- resolved semantic blueprint，用于 Phase 9 构造 fresh `BoundStatement/Query` 视图，包含列身份、类型、alias 解析结果和输出元数据。
- physical execution blueprint，用于 Phase 10 直接构造 fresh Plan/Executor state，覆盖 SELECT、INSERT、UPDATE、DELETE 的可缓存热点结构。
- `CompiledStatement` descriptor 与一个或多个 `JitCodeKey`；可用 weak hint 加速查询，但不能强绑定机器码生命周期。

`shadow`、`parser`、`analyzer`、`full` 是同一 entry 的最大 bypass 级别，不是三张相互独立的 AST/Query/Plan cache。readiness 升级时构造一个新的完整 immutable entry 并原子替换旧 `shared_ptr`，不能在已发布 entry 内 lazy 修改 optional 字段。现有 SELECT `PhysicalPlanTemplate` 可在迁移期作为蓝图实现基础；`full` 稳定后应并入这一所有权模型，避免重复容量、失效和统计体系。

### 8.2 最终热命中路径

1. Lexer 完整扫描 SQL，生成 owned canonical token stream、`LexicalParamBlock` 和 `StatementShapeKey`。
2. 使用 key 的 digest + canonical bytes，并在同一 catalog/planner epoch 下查找完整模板。
3. 先执行 Parser 等价的 lexical binding/range/negative-literal 校验并取得 statement kind；这类错误仍发生在事务建立前，与当前 `parse_sql()` 顺序一致。
4. 使用模板 statement kind 走当前 checkpoint/load 特判和 `SetTransaction` 逻辑。随后才执行 Analyzer 等价的 type/cast/length/DATETIME 绑定和 physical blueprint 实例化；这类错误仍由当前事务异常路径处理。
5. 在 semantic binding 和实例化前后核对 generation，发现 DDL/配置竞态立即放弃 entry 并走完整流水线。直接创建本次独有的参数、fresh AST owner（需要时）、Plan/Executor/iterator/aggregate state 和输出 metadata；不得调用 `parse_sql()`、`Analyze::do_analyze()` 或 `Planner::plan_query()`。
6. 按模板携带的 `JitCodeKey` 查询 `JitCodeCache`。代码 miss 时使用解释 Executor 并可重新排队编译，不能因此失去前端 bypass；代码 hit 时由本次执行持有强引用并调用 Kernel。
7. Portal、MVCC/SSI、锁、WAL、异常映射和输出仍按现有顺序执行。

fallback 必须区分事务建立前后：前置 lexical/lookup 失败可从完整请求入口重走；`SetTransaction` 之后发现 generation/blueprint mismatch 时，只在当前已建立的事务上下文内补跑 Parser/Analyzer/Planner，不得再次调用 `SetTransaction`。实现时把完整前端提取为显式接收“事务是否已建立”的单次 helper，并以调用计数证明 miss/race 不会重复解析、重复建事务或执行两次语句。

因此 `full` 命中只能让 Parser/Analyzer/Planner 的函数样本为零，不能让 Lexer、cache lookup、binder、蓝图实例化或 Executor 控制流为零。

### 8.3 渐进模式

| `RMDB_STATEMENT_CACHE` | 命中后的行为 | 目的 |
| --- | --- | --- |
| `off` | 完整旧流水线；可完全关闭模板设施作为基线。 | 正确性/性能基线与紧急关闭。 |
| `shadow` | 生成 key、查询/构建模板，但仍执行 Parser + Analyzer + Planner；比较 shape、绑定结果、语义蓝图和物理蓝图，不影响结果。 | 在零行为风险下验证归一化和所有权。 |
| `parser` | 从 parsed skeleton 绑定/实例化 AST，跳过递归下降 Parser；Analyzer + Planner 仍执行。 | 单独证明 Parser bypass 正确且有收益。 |
| `analyzer` | 从 resolved semantic blueprint 绑定语义对象，跳过 Parser + Analyzer；Planner 仍执行。 | 单独证明语义绑定、错误和 DDL 失效正确。 |
| `full` | 从 physical execution blueprint 实例化执行状态，跳过 Parser + Analyzer + Planner。 | 最终生产热路径。 |

模式只限制允许使用的最高 readiness；低 readiness、miss、不支持语句或任何校验失败都走完整旧流水线。不能从 `parser` 命中直接跳到尚未通过门禁的 `full` 蓝图。

### 8.4 容量、失效与并发

- 缓存同时设置 entry count 和 owned template bytes 上限；初始 entry 上限可沿用现有 256，再由真实 shape 数和内存数据校准。它与 16 MiB RX `JitCodeCache` 分别记账、分别淘汰。
- global `catalog_generation`、template format version 和 planner knob epoch 参与 key；publish 前、命中后实例化前再次校验。第一版宁可在任意 DDL 后全部自然 miss，也不能复用旧列 offset/type/index choice。
- entry 以 `shared_ptr<const StatementTemplate>` 发布，淘汰只移除 map 引用；本次实例化持有强引用。entry 内不得 lazy 修改共享 vector/AST/Plan。
- 同一 key 使用 single-flight 建模。leader 运行完整流水线并发布；并发 follower 不应长时间阻塞用户请求，可继续完整流水线并在 publish 时丢弃重复结果。
- DDL/管理/事务控制等低频或尚未建立安全蓝图的语句明确标记 non-cacheable；它们始终走完整流水线。目标热点 SELECT/DML 必须逐类通过门禁，不能用 TPCC 表名白名单决定资格。
- malformed SQL、额外/缺失 token 和词法错误不会命中合法 canonical bytes；literal 相关的范围/cast/长度错误由绑定 recipe 保持旧错误。shadow mismatch 立即隔离该 key，并记录匿名 digest 与原因。

## 9. JIT IR 与调用 ABI

### 9.1 IR 原则

IR 是受限、类型化、无任意跳转的内部表示。用户 SQL 只能通过 analyze/plan 产生合法 IR，不能直接提交机器码或 AsmJit 指令。

首批操作集合：

- `LoadI32`, `LoadF64`, `LoadBytes`：从已验证的 tuple offset 读取。
- `LoadParamI32`, `LoadParamF64`, `LoadParamBytes`。
- `CastI32ToF64`，以及当前代码明确允许的赋值转换。
- `CmpEq/Ne/Lt/Gt/Le/Ge`，覆盖 numeric 和 bytes 语义。
- `AndThen`：保持当前从左到右短路。
- `CopySpan`, `StoreI32`, `StoreF64`, `StoreBytes`。
- `Add/Sub/Mul/Div` 与明确的 error status。
- `CountStep`, `SumStep`, `MinStep`, `MaxStep`, `AvgStep`。
- `ReturnMatch`, `ReturnStatus`。

首版 IR 不需要 SSA 优化框架。Builder 在生成时已知道固定 offset/type；只做常量折叠、连续 copy span 合并、重复 load 消除和不可达分支删除即可。避免为了一个小型表达式编译器引入过度抽象。

### 9.2 IR verifier

任何 AsmJit 发射前必须验证：

- `offset + len <= tuple_len`，目标写入不越界。
- `INT` 长度为 4、`FLOAT` 长度为 8；bytes 长度和参数边界合法。
- 每个 parameter/state index 存在且类型一致。
- 算术、比较和转换组合属于当前 SQL 语义允许集合。
- 指令数、参数数、copy 总量均低于全局上限，防止超大语句制造代码膨胀。
- Kernel 中没有未受控循环；字符串比较首版通过稳定 C helper 完成，之后才考虑受限内联/SIMD。
- 所有 return path 都写入确定的 status/result。

### 9.3 稳定调用 ABI

建议定义一个版本化、标准布局的 `JitCallFrame`，只包含：

- `tuple0`、可选 `tuple1` 及其长度。
- `output` 及其长度。
- `params`、参数数量。
- 可选 POD aggregate state。

Kernel 使用 `noexcept` C ABI 形式，返回 `JitStatus`，结果通过 frame 中的 POD 字段传出。生成代码中不构造 C++ 对象、不分配内存、不抛异常、不调用虚函数。

`JitStatus` 至少区分 `OK`、`DIVISION_BY_ZERO`、`INVALID_INPUT` 和 `HELPER_ERROR`。C++ wrapper 在副作用发生前把非 OK 状态映射为现有异常。Predicate/Projection 的纯函数可在 debug sampled verification 中同时运行解释器并比较结果。

### 9.4 语义细节

- Numeric 比较必须与当前先提升到 `double` 的行为一致，包括 `INT/FLOAT` 混合比较。
- 浮点比较要显式处理 unordered/NaN，使六种运算符与 C++ 当前结果一致。
- 字符串和 DATETIME 保持当前固定列内 NUL 截断、字典序及 literal 长度语义；不能用裸定长 `memcmp` 悄悄改变尾部零字节行为。
- Update 的除零、赋值 cast、字符串清零/截断行为逐项对照现实现；整数溢出边界由差分测试固定，不在 JIT 中另创新规则。
- 当前系统没有 SQL NULL 数据模型，JIT 不自行引入三值逻辑；未来增加 NULL 时必须升级 IR/ABI 版本。

## 10. AsmJit 后端与可执行内存

### 10.1 构建集成

1. 直接使用当前 `deps/asmjit` clone，不增加下载脚本、`FetchContent`、包管理器查找或另一个 AsmJit 来源。
2. 将根 CMake 最低版本统一到 3.24，以满足当前 AsmJit 1.21.0 的 `cmake_minimum_required()`；不通过降级依赖或维护本地补丁规避该要求。
3. 在 `deps/CMakeLists.txt` 调用 `add_subdirectory(asmjit)`。调用前固定 `ASMJIT_TEST=OFF`，生产构建推荐 `ASMJIT_STATIC=ON`，避免构建 AsmJit 自身测试或引入运行期共享库部署要求。
4. 新建独立 `jit` static library，并调用 `target_link_libraries(jit PRIVATE asmjit::asmjit)`；`execution` 只依赖 `jit`，`jit` 不反向依赖具体 Executor，避免循环依赖，也不向 JIT 公共头文件泄漏 AsmJit 类型。
5. 最终提交前把当前 `deps/asmjit` 源码目录纳入 RMDB 仓库跟踪，同时保留当前 commit、版本信息和 Zlib license；CI 只使用仓库内源码构建并验证依赖完整，不访问网络获取浮动版本。

### 10.2 代码生成流程

每次编译使用独立 `CodeHolder` 和 Compiler：

1. 以 `JitRuntime::environment()` 和 host CPU features 初始化 `CodeHolder`。
2. 安装 `ErrorHandler`；Debug 构建开启 assembler/intermediate validation 和可选 `StringLogger`。
3. 从已验证 IR 发射函数，检查每个 AsmJit `Error`。
4. `Compiler::finalize()` 后调用 `JitRuntime::add()` 获取函数地址。
5. 发布 immutable `JitCode`；其析构最终调用 `JitRuntime::release()`。

AsmJit `JitAllocator` 的 alloc/release/write 是 thread-safe，但 `CodeHolder`/Compiler 不共享。编译工作可在一个有界 worker 中串行完成，先避免 register allocator 和 publish 路径的额外并发复杂度；若 profile 证明编译队列成为瓶颈，再扩到少量 worker。

### 10.3 架构策略

- 第一生产目标建议为 x86-64，使用 `asmjit::x86::Compiler` 和 baseline x86-64 指令，保证当前机器可运行。
- IR、调用 ABI、cache 和 hotness 必须架构无关。
- 非 x86-64 首版报告 `unsupported architecture` 并全量解释执行，不能影响 server 启动。
- AArch64 作为独立后续 gate，使用 `a64::Compiler`；在具备真实 AArch64 CI 前不得宣称支持。
- 若以后生成 SSE/AVX 特化版本，CPU feature tier 必须进入 cache key；首版不应为微小收益增加多版本代码复杂度。

### 10.4 安全与 W^X

- 使用 AsmJit `JitRuntime`/`JitAllocator` 管理 RW->RX 或 dual mapping，不自行 `mmap(PROT_WRITE|PROT_EXEC)`。
- 生成代码只能访问 frame/verifier 允许的范围，只能调用白名单稳定 helper。
- 可执行内存和条目数同时设硬上限；队列满、分配失败或 hardened 环境禁止 JIT 时回退。
- 不从磁盘加载未认证代码，不把 SQL 文本拼成汇编。

## 11. 热度检测、编译决策与状态机

### 11.1 每个 JIT code shape 的统计

- `execution_count`：语句结构被执行次数。
- `tuple_evaluation_count`：解释谓词/投影/update/aggregate step 的调用量。
- `interpreted_ns`：低采样率记录的纯计算时间 EWMA，避免每 tuple 都读时钟。
- `last_seen`、`last_compiled`、`compile_ns`、`code_bytes`。
- `cache_hits`、`fallbacks`、`compile_failures` 和 failure reason。

### 11.2 初始 auto 策略

以下是需要由 Phase 0/微基准校准的通用起始值，不是 TPCC 特例：

- 至少执行 32 次，且累计至少 256 次可 JIT tuple evaluation。
- 估算 `预计未来调用量 * 单次可节省时间 > 编译耗时 * 2` 时进入队列。
- 编译队列最多 64 个 shape；代码缓存最多 256 个条目且不超过 16 MiB。
- 编译失败进入 60 秒 cooldown；相同 IR/环境下不在每次请求重试。
- 一次巨大查询可在累计 tuple 阈值后排队，但当前查询继续解释执行；不在扫描中途切换 Kernel，避免状态复杂化。

模式配置：

- `RMDB_JIT=off`：完全禁用，作为回归基线。
- `RMDB_JIT=auto`：使用热度和收益判断，最终生产默认候选。
- `RMDB_JIT=force`：只要支持就同步/立即编译，限测试和差分验证，不建议生产。

只暴露少量必要调优项，如 cache bytes 和 min executions；其他阈值先保持内部常量并由数据校准，避免形成难以维护的配置面。

### 11.3 状态机

```text
COLD -> OBSERVING -> QUEUED -> COMPILING -> READY
                    |            |           |
                    |            v           v
                    +------ FAILED_COOLDOWN  EVICTED
                                   |
                                   +------> OBSERVING (epoch/config 改变后)
```

- 使用原子状态/CAS 保证同一 key 只有一个编译任务（single-flight）。
- `READY` 发布使用 release/acquire；读路径不拿编译锁。
- cache lookup 可用 `shared_mutex`，LRU touch 采用采样或分批更新，避免 16 worker 下每次命中争同一写锁。
- 队列满时保持 `OBSERVING`，不能阻塞用户线程。

## 12. JitCodeCache、失效与并发生命周期

### 12.1 Cache key 和失效

- key 包含当前 `SmManager::get_catalog_generation()`，任何 DDL/open/close 后自然 miss。
- publish 前再次读取 generation；编译期间发生 DDL 时丢弃结果。
- generation 改变后旧条目可惰性清理，不必在 DDL 持锁同步释放全部机器码。
- JIT IR/ABI、CPU feature tier、planner knob 或 JIT 配置改变时增加对应 epoch。
- 第一版用全局 catalog generation，虽会多失效但逻辑简单安全；只有 profile 证明 DDL 频繁造成问题时才做 per-table epoch。

### 12.2 淘汰

- LRU 同时受 entry count 和 executable bytes 限制。
- map 中保存 `shared_ptr<const JitCode>`；Executor/本次执行也持有一份。
- 淘汰只移除 map 引用，不直接释放正在执行的地址。
- `JitCode` 析构在没有在途调用后调用 `JitRuntime::release()`；不得在持 cache mutex 时调用可能较慢的释放。

### 12.3 启停顺序

1. 数据库打开后初始化 `StatementTemplateCache` 和 `JitManager`，确认 host architecture/runtime 可用。
2. 接收请求前启动编译 worker；模板建模仍由正常请求的完整流水线产出，不另起无界 worker。
3. server 停止接受连接后，先禁止新 template lookup/publish，再停止接收新编译任务并 join worker。
4. 等待所有 client execution scope 退出，再分别清空 template/code cache。
5. 最后销毁 `JitRuntime`，随后关闭数据库。

当前 detached client 线程没有统一 join 机制，这是 JIT 安全关闭前必须核对的既有生命周期风险。实现时可给 active client scope 增加计数/condition variable；不得在仍有调用时依赖全局对象静态析构顺序。

## 13. Executor 集成边界

### 13.1 Predicate

- 在 `AbstractExecutor` 邻近位置增加轻量 `PredicateEvaluator`：持有解释条件、地址缓存、可选 `shared_ptr<JitCode>` 和执行参数视图。
- SeqScan、IndexScan、Filter 改为调用同一 evaluator；evaluator 内部直接分派到 JIT 或现有 `conditions_match()`。
- Kernel 只在 `GetVisibleTuple()` 成功后运行；`record_predicate_read()`、`record_tuple_read()` 的时机不得改变。
- 空条件走现有快速 true path，不值得 JIT。

### 13.2 Join

- Join Predicate Kernel 接收 left/right 两个 tuple 指针，直接使用各自 offset。
- 保留 INLJ 的 `set_lookup_key()`、index bounds 和 child iterator 行为。
- 首版只替代 `is_condition()` 内的纯比较，不改变 join order 或输出 tuple materialize。

### 13.3 Projection

- Builder 把投影列转换为 source/destination span，合并连续区域。
- 小定长 span 发射直接 load/store；大 span 调用白名单 `memcpy` helper，具体阈值由微基准确定。
- 输出 buffer 仍由 `ProjectionExecutor` 拥有，Kernel 不分配、不改变列名元数据。

### 13.4 Update

- `UpdateExecutor` 先完成可见性复查和 record X lock，再把 old/new tuple buffer 交给纯 Update Kernel。
- Kernel 成功后才执行 index/WAL/undo/heap 副作用；失败状态由 wrapper 转为现有异常，确保没有部分更新。
- predicate 的 RC 重检仍走同一个 PredicateEvaluator，不能因 JIT 绕过并发控制。
- SELF_ADD/SUB/MUL/DIV、ASSIGNMENT、跨 numeric cast 和字符串 assignment 分阶段全覆盖。

### 13.5 Aggregate

- 第一阶段只 JIT global numeric aggregate transition 和 HAVING predicate。
- group hash table、`CellValue`/`std::string` 管理和 group materialization 保留 C++。
- 引入独立 POD transition state 后，再让 Kernel 一次更新多个 aggregate，减少 tuple 反复解码。
- `COUNT(*)` 当前已有 cursor-only 快速路径，先以 benchmark 对比，不能让 JIT 反而替换更快的现有 shortcut。

### 13.6 Fusion

只有满足以下条件才构建 fused tuple Kernel：

- 子树仅包含纯 Filter、Projection、可支持的 Aggregate transition。
- 不跨越 MVCC/SSI、Limit early-stop、Sort、Union、Index cursor 或任何副作用边界。
- 输出布局在编译时固定，参数完全来自 frame。
- fused 与未融合路径通过同一 IR interpreter 做差分。

首选 `Scan visibility -> fused filter/project -> C++ consumer` 和 `Scan visibility -> fused filter/agg step`，不生成一整个 scan loop。这样能减少 per-tuple 虚调用，又保持事务/存储逻辑可审计。

## 14. 文件级实施清单

以下是预计改动面；每一项都应在对应里程碑才落地，避免一次性大改。

| 路径 | 计划改动 |
| --- | --- |
| `CMakeLists.txt` | 解决 CMake 3.24 要求，加入 JIT build option。 |
| `deps/CMakeLists.txt` | 对当前 clone 设置固定构建选项并调用 `add_subdirectory(asmjit)`。 |
| `src/CMakeLists.txt`、`src/execution/CMakeLists.txt` | 增加 `jit` 以及后续 `statement_template` target，并保持单向依赖。 |
| `src/parser/token_stream.h/.cpp` | 在现有 Lexer 上构建拥有 identifier/string 存储的 token/parameter stream、canonical bytes 和 `StatementShapeKey`；不使用 regex。 |
| `src/parser/parser.h/.cpp`、`src/parser/ast.h` | 让 Parser 可消费 reusable token stream；补齐 statement skeleton 的不可变表示/实例化，保留 `parse_sql()` 作为公开入口和 oracle。 |
| `src/jit/jit_types.h` | POD ABI、status、配置、metrics 和版本号。 |
| `src/jit/jit_ir.h/.cpp` | IR node、builder、canonical serializer、verifier 和解释器 oracle。 |
| `src/jit/jit_compiler.h/.cpp` | AsmJit codegen、ErrorHandler、host feature gate、`JitCode` RAII。首版可将 x86 backend 保持在同一实现文件，待 AArch64 再拆分。 |
| `src/jit/jit_manager.h/.cpp` | hotness、single-flight queue、cache、LRU、epoch 和统计。 |
| `src/jit/jit_plan_builder.h/.cpp` | 遍历 `Plan`，构建 statement descriptor、Kernel IR、fingerprint 和 parameter binding map。 |
| `src/cache/statement_template.h/.cpp` | 定义 immutable parsed/semantic/physical blueprint、parameter descriptor、binder 和 per-execution `BoundStatement`。 |
| `src/cache/statement_template_cache.h/.cpp` | Parser 前 lookup、readiness/mode、single-flight publish、有界 LRU、catalog/planner epoch、熔断和统计；不依赖 AsmJit。 |
| `src/analyze/analyze.cpp/.h` | 从成功 Analyze 结果构建 resolved semantic blueprint，并提供不共享 mutable `Query` 的实例化/绑定路径。 |
| `src/common/context.h` | 仅增加前置声明指针或 execution-scope 句柄，不让 common 依赖 AsmJit。 |
| `src/rmdb.cpp` | 先接 JIT execution scope；Phase 7 后在 `parse_sql()` 前接 template lookup、逐级 bypass 和调用计数，保持事务建立/异常/协议顺序。 |
| `src/portal.h` | 从 fresh Plan 或 physical execution blueprint 构造 fresh Executor，并把相应 Kernel/参数视图注入节点。 |
| `src/execution/executor_abstract.h` | 抽出可解释/JIT 双路径 evaluator，保留现有 compare 作为权威回退。 |
| Scan/Filter/Join executors | 接入 Predicate Kernel，不移动 MVCC、SSI 或 cursor 边界。 |
| Projection/Update/Aggregate executors | 按阶段接入对应 Kernel，保留 C++ resource/error wrapper。 |
| `src/optimizer/planner.cpp/.h` | 复用/提取 canonical shape 和 `PhysicalPlanTemplate` 实例化思路，扩展 SELECT/DML physical blueprint；不让任一 cache 持有 mutable Plan。 |
| `src/system/sm_manager.*` | 首版只复用现有 catalog generation；若无缺口不改代码。 |
| `test/jit/*.cpp` | 新建独立 `jit_test` target，覆盖 IR、codegen、cache、并发与差分。 |
| `test/statement_template/*.cpp` | 新建 `statement_template_test`，覆盖 token shape、分层 binder/blueprint、模式调用计数、并发、失效和淘汰。 |
| `test/e2e/slt/jit.slt` 或等价 fixture | 通用 SQL 结果/错误/DDL 失效测试；预期输出格式与现有完全一致。 |
| `benchmark/tpcc` 与 benchmark script | 增加不影响默认行为的 deterministic run seed、JIT/template A-B 参数和内部统计采集；每轮可靠执行报告落盘与 allowlist cleanup，不写 `output.txt` 新字段。 |
| `scripts/run_jit_validation.sh` | 统一生成 run_id、磁盘 preflight、执行测试、提取指标、原子追加结果文档并在 `trap` 中按 manifest 清理当轮产物。 |
| `scripts/perf_flamegraph.sh` | 直接以普通用户运行 `perf record`/`perf script`，不调用 `sudo`；报告提取完成后删除大体积 `perf.data`/folded 中间文件。 |
| `JIT_PERFORMANCE_RESULTS.md` | 单一、追加式性能与验证记录；保存每轮配置、结果、关键指标、对比结论和清理状态，不保存大体积原始采样。 |

## 15. 分阶段实施与验证门禁

所有 Phase 的测试和 benchmark 统一遵守以下协议，不能等最终调优时再补数据：

1. 每次运行先生成唯一 `run_id`，记录 commit/worktree 状态、时间、主机/CPU、编译类型、Phase、完整命令、JIT/template 模式、数据规模、seed、workers、warmup/measure 和关键 cache/planner 配置。
2. 运行结束后，无论成功或失败，都先把 pass/fail、测试耗时、吞吐/延迟、CPU、四热点、两个 cache、编译/建模和内存等本轮可用指标追加到仓库根目录 `JIT_PERFORMANCE_RESULTS.md`；缺失指标显式写 `N/A + 原因`，禁止只记录表现最好的一轮。
3. 报告成功落盘后再执行 allowlist cleanup：终止/等待当轮 server/client/perf 进程，删除当轮数据库目录、temporary round JSON、socket/lock、`perf.data`、folded stack、临时日志、core/trace 和其他已登记的大体积产物。保留结果文档以及明确选为基线/回归证据的小型汇总或 SVG。
4. cleanup 必须通过 `trap` 覆盖成功、失败、SIGINT/SIGTERM，并且只删除由本轮创建且位于已知根目录的路径；不得使用可能匹配用户数据库、源码或 CSV 基准输入的宽泛 glob。
5. 每轮开始前记录 `df`/相关目录 `du` 并检查可用空间；每轮结束后记录清理前后 bytes 和 cleanup 状态。若报告未落盘、清理失败、仍有当轮进程/目录，或剩余空间低于安全门槛，停止后续轮次并把该轮标为失败，防止循环测试耗尽磁盘。
6. 一个 Phase 的全部门禁通过、结果文档落盘且 cleanup 通过后，必须为该 Phase 创建一个独立 Git checkpoint commit；未通过门禁不得用提交掩盖未完成状态。默认每个 Phase 收敛为一次提交，并保留 Phase 0-12 的边界，不在最终阶段自动 squash。

`JIT_PERFORMANCE_RESULTS.md` 是唯一长期累积的文字记录。原始 perf/日志只允许按固定数量保留最近失败样本并设置容量上限；一旦关键指标、错误摘要和所需 flamegraph 已提取，其余原始文件立即清理。

### Git 提交与行尾约定

- 每个 checkpoint 只 stage 当前 Phase 的实现、测试、计划/结果文档和必要构建文件；先用 `git status --short` 核对，不纳入 Phase 开始前已有的无关修改、日志、数据库、perf 产物或其他 untracked 文件。
- 提交信息使用现有 Conventional Commit 风格并标明阶段，例如 `perf(jit): complete phase 0 baseline`、`feat(jit): complete phase 4 predicate kernels`、`feat(cache): complete phase 10 planner bypass`。
- 当前 `deps/asmjit` clone 的源码跟踪仍按既定约定留到用户确认的最终提交；各 Phase checkpoint 不得因为 `git add` 范围过宽而提前或部分纳入该目录。
- CRLF 与 LF 的纯行尾差异一律视为非语义噪声，不进行原因分析、不阻塞门禁、也不为此创建格式化提交。review 使用 `git diff --ignore-space-at-eol` 查看语义改动，并可用 `git -c core.whitespace=cr-at-eol diff --check` 检查真实 whitespace 问题。
- 不为“统一行尾”修改 `.gitattributes`、全局/仓库 `core.autocrlf` 或批量重写已有文件；编辑时尽量保持文件现状。若 diff 只有 CRLF/LF 变化，则不 stage 该变化；测试结果也不得因平台换行差异被判为功能回归，但 `output.txt` 的既有字节格式仍受本文兼容规则约束。

### Phase 0：建立可重复基线和收益上限（M，必须先完成）

实施项：

1. Release 构建下分别记录 1/8/16 worker、固定 warehouses 的五轮 TPCC 数据。
2. 给 Lexer/normalize、Parser、Analyzer、Planner、Portal/blueprint instantiate、Executor 分别做低开销时间/cycle 采样，输出到 server log 或独立 metrics 文件。
3. 使用 `perf stat`/flamegraph 同时确认 Parser/Analyzer/Planner 三个前端热点，以及 `conditions_match`、Join 比较、Projection copy、Update 算术、Aggregate transition 等 Executor 热点的真实占比。
4. 为 Go runner 增加可选固定 run seed；默认行为保持不变。
5. 建立 `JIT_PERFORMANCE_RESULTS.md` 的固定记录模板和 test wrapper；验证报告写入发生在 cleanup 前，并复用/扩展 `benchmark-clean` 的显式路径清理。

门禁：

- 五轮 tpmC 变异系数建议小于 5%，否则先修 benchmark 稳定性。
- 分别给出“Executor JIT 可消除”和“完整模板命中可绕过”的 CPU cycles 占比。若前者低于 15%，缩减未证明有收益的 Kernel/fusion 范围并调整整体 20% 目标，但不取消 Phase 7-11 的前端缓存目标；若后三段并非热点，也仍完成正确性设计并如实报告收益上限。
- 人为制造成功、测试失败和中断三种运行，三者都必须先在结果文档形成记录，再清空当轮大体积产物；连续多轮后磁盘占用回到可解释的稳定基线。

### Phase 1：AsmJit 构建与生命周期骨架（S-M）

实施项：

1. 完成 CMake 集成、静态链接、license 和 unsupported-arch gate。
2. 实现 `JitCode` RAII 和最小 `JitRuntime` wrapper。
3. 用 test-only IR 生成一个参数化整数函数，反复调用并安全 release。
4. 验证 Debug diagnostics、编译错误返回和可执行内存失败回退。

门禁：

- `make build`、`make test` 全绿。
- smoke test 在 x86-64 正确执行；强制分配/compile 失败时 server 测试不崩溃。
- ASMJIT 不可用的构建仍能生成完全解释执行的 RMDB。

### Phase 2：IR、fingerprint、参数绑定和解释 oracle（M-L）

实施项：

1. 定义版本化 IR/ABI、verifier、canonical serializer。
2. 从 `ScanPlan`/`FilterPlan`/`JoinPlan` 条件构建 Predicate IR。
3. 同时生成 deterministic parameter slots 和当前执行 binding。
4. 实现 IR interpreter，结果与 `AbstractExecutor::compare()` 对照。
5. 接入 catalog generation，但此阶段仍只解释 IR，不执行机器码。

门禁：

- 不同 literal、相同结构得到同 key 和不同参数块。
- operator/type/column offset/plan tag/schema generation 任一变化都不能错误命中。
- deterministic 随机生成至少 10 万组合法 tuple/条件，IR interpreter 与现实现结果一致。
- malformed IR 全部被 verifier 拒绝，且无越界访问。

### Phase 3：自动 hotness、异步 single-flight 和有界 cache（M）

实施项：

1. 实现 `off/auto/force`、统计、状态机和 bounded queue。
2. 实现 entry/bytes 双上限 LRU、共享所有权淘汰和 failure cooldown。
3. publish 前二次校验 epoch；实现 database open/close/DDL 后的惰性失效。
4. 加入 server shutdown drain，解决编译 worker 与 detached client 的销毁顺序。

门禁：

- 32 个并发线程请求同一 key 只编译一次。
- 一边执行一边淘汰/DDL，不出现 UAF、旧代码命中或死锁。
- queue full、OOM、unsupported、compile error 都透明回退。
- 长时间随机 key 压测后 entry 数和 RX bytes 不越界。

### Phase 4：Predicate Kernel 上线（L，首个端到端 JIT）

实施项：

1. 生成 INT/FLOAT 单类型与混合比较、AND 短路。
2. 通过 helper 正确支持 STRING/DATETIME；验证后再考虑内联优化。
3. 接入 SeqScan、IndexScan residual、Filter 和 NLJ。
4. 增加低比例 shadow verification：运行 JIT 后抽样再跑解释器，只记录 mismatch 并立即对该 key 熔断。

门禁：

- 六种比较运算、两种 tuple source、parameter source、边界 offset、空字符串、无 NUL 定长串、NaN/Inf 全有差分用例。
- `RMDB_JIT=off`、`force` 对全量 e2e 结果和 `output.txt` 做 byte-for-byte 比较。
- SERIALIZABLE/RC/SI 测试证明 SSI 读跟踪和可见性调用次数未变化。
- Predicate 微基准达到第 2.2 节的 cycle 目标，否则不默认开启。

### Phase 5：Projection 与 Update Kernel（L）

实施项：

1. 实现 copy span 合并和固定偏移 projection。
2. 实现 Update ASSIGNMENT、SELF_ADD/SUB/MUL/DIV 和允许的 numeric cast。
3. 把 Kernel 状态映射到既有异常，确保所有数据副作用在成功后发生。
4. 针对 new_order/payment/delivery 的结构做观测验证，但测试断言只使用通用 SQL 和 IR，不出现 TPCC 表名特判。

门禁：

- 投影重排、重复列、相邻/非相邻 span 和各列类型差分一致。
- Update 的正负数、零、极值、cross-type、除零、字符串边界与现实现一致。
- 事务 rollback、索引 key 更新、WAL/recovery 和并发 write conflict 测试全绿。
- 运行 `make benchmark-random-kill`，恢复后一致性不变。

### Phase 6：Aggregate 与安全融合（L）

实施项：

1. 先实现 global `COUNT(*)` 和 numeric `SUM/MIN/MAX/AVG` transition。
2. 支持一个 Kernel 更新多个 aggregate，随后加入 HAVING predicate。
3. 在不跨副作用边界的情况下实现 filter+projection、filter+aggregate transition 融合。
4. profile 后决定是否扩展 group-by transition 和 string min/max；哈希表仍由 C++ 持有。

门禁：

- 空输入、单行、多行、负数、混合 numeric、多个 aggregate、HAVING 的结果均差分一致。
- 当前 `COUNT(*)`/MIN index shortcut 不能退化。
- 融合前后 Executor row count、RID 读取、SSI 记录和 Limit early-stop 相同。
- 只有融合微基准证明额外收益时才保留对应路径。

### Phase 7：owned token 归一化与 shadow lookup（M-L，Phase 6 后必须实施）

实施项：

1. 在现有 Lexer 上增加 `OwnedTokenStream`，让 Parser 可消费同一 token stream，避免为缓存另写词法器或 regex normalizer。
2. 在 `parse_sql()` 前生成 `StatementShapeKey` 和 `LexicalParamBlock`，正确拥有 identifier/string，保留 token 顺序、类型、一元负号及 EOF。
3. 成功走完旧 Parser/Analyzer/Planner 后先构建 `SHAPE_ONLY` entry，记录 lexical parameter recipe、statement kind 以及旧 AST/Query/Plan 的 canonical reference digest，并 single-flight publish；先覆盖通用热点 SELECT/INSERT/UPDATE/DELETE，其他语句明确 non-cacheable。
4. `shadow` 命中仍执行完整旧流水线，比较 token shape、lexical 参数提取和 reference digest；Phase 8-10 每增加一层 blueprint，就在 shadow 中增加该层的 fresh instantiate 差分后才提升 readiness。mismatch 隔离该 key。
5. 增加 Lexer/lookup/build/compare 计时，以及 template hit/miss/readiness/mismatch/bytes 指标。

门禁：

- `parse_sql(sql)` 与 `Parser(OwnedTokenStream)` 对全部 parser test 产生等价 AST 或完全相同的 `ParseError` 文本。
- 仅空白和 literal 不同得到同 shape；identifier、token 类型/顺序、运算符、标点或 planner/catalog epoch 改变不得错误命中。
- 正数、负数、算术减号、`INT_MIN`、整数/浮点溢出、bool、空/最大字符串和未闭合字符串均保留现有行为。
- `SHAPE_ONLY` 不持 AST/Query/Plan 指针或请求视图；`shadow` 跑全量单元/e2e/TPCC warmup 无 shape/reference mismatch、无输出差异，DDL race、并发 publish 和淘汰测试无旧 entry/UAF，缓存 bytes 不越界。

### Phase 8：Parser bypass（M）

实施项：

1. 补齐 immutable parsed statement skeleton 及其 fresh AST/绑定实例化器，不共享 AST `unique_ptr` 或请求缓冲区视图；先在 `shadow` 中与旧 Parser AST 差分，再原子发布 `PARSED_READY` entry。
2. 在 `parser` 模式的 `PARSED_READY` 命中上绑定 lexical 参数并跳过递归下降 Parser；Analyzer 和 Planner 仍照常执行。
3. 将 Parser 中的范围、负号和 literal display/error 规则收敛为 Parser 与 binder 共用的 helper，避免复制两套语义。
4. miss、non-cacheable、DDL/knob 失效和内部 recipe mismatch 一律走完整旧流水线。

门禁：

- test-only 入口计数证明有效命中时 Parser 为 0、Analyzer 为 1、Planner 为 1；miss 时三者均按旧路径各执行一次。
- fresh AST 与旧 Parser AST 做结构差分，随后生成的 Query/Plan 也一致；并发执行同一模板没有交叉污染。
- malformed SQL 和 literal overflow/负号/string 错误文本不变；`parser` 与 `off` 的全量 e2e 和 `output.txt` byte-for-byte 一致。
- 单独报告 Parser cycles/transaction 的下降和 lookup/bind 开销；收益未达预期不能据此跳过 Analyzer/Planner bypass。

### Phase 9：Analyzer bypass（L）

实施项：

1. 从已验证 Query 构建 resolved semantic blueprint，固化列/表身份、alias 解析、类型、聚合/分组/排序/LIMIT/UNION 语义、输出 metadata 和参数 conversion recipe；先在 `shadow` 中差分 fresh Query/Plan，再原子发布 `ANALYZED_READY` entry。
2. `analyzer` 模式命中时直接绑定 fresh semantic statement，跳过 Parser + Analyzer；Planner 仍照常执行，并获得当前执行独有的 AST owner、`Query::parse`、Value/condition/set-clause 容器，不能指向模板内部对象。
3. 将 literal 到目标类型的 range/length/cast/DATETIME 校验复用现有 helper，并保持旧异常类型和文本。
4. 每次 lookup/publish/实例化校验 catalog generation；DDL 后不得使用旧 ColMeta、offset、index 或 alias 解析结果。

门禁：

- 有效命中时 Parser/Analyzer 入口计数均为 0、Planner 为 1；Query/Plan 与完整流水线逐字段或 canonical bytes 一致。
- 覆盖同名/歧义列、表 alias、JOIN、aggregation/group/HAVING/order/LIMIT/UNION，以及 INSERT/UPDATE/DELETE 的通用参数绑定；未支持 shape 必须稳定 miss。
- literal range、CHAR 长度、numeric conversion、DATETIME 和 DDL 后 schema/type 变化产生与 `off` 相同的结果或错误。
- ASan/TSan 下并发绑定、eviction 和 DDL race 无悬空引用、共享 mutation 或旧模板命中。

### Phase 10：Planner bypass 与完整 execution blueprint（L）

实施项：

1. 将现有 SELECT `PhysicalPlanTemplate` 思路扩展为覆盖目标 SELECT/DML 的 immutable physical execution blueprint，并纳入单一 `StatementTemplate` 所有权；先在 `shadow` 中差分 fresh physical plan/execution metadata，再原子发布 `PLANNED_READY` entry。
2. `full` 模式命中时直接实例化 fresh Plan/Executor/iterator/aggregate state，跳过 Parser + Analyzer + Planner；只复用不可变 layout、operator choice、binding recipe 和输出 metadata。
3. 把 index/seq scan 选择、join order、projection/aggregate/sort/LIMIT/UNION/DML 节点及所有影响规划的 knob 编入蓝图/key。
4. 保持“Parser 等价校验 -> statement kind/事务建立 -> Analyzer 等价绑定 -> Portal/MVCC/SSI/lock/WAL/undo/index/heap/output”的错误与副作用顺序；不得缓存 Executor 或本次 Context/Transaction/Value。

门禁：

- 有效 `full` 命中时 Parser、Analyzer、Planner 入口计数全部为 0；每次执行仍得到地址/状态独立的 Plan/Executor 树。
- 蓝图实例化计划与旧 Planner canonical plan 一致；planner knob、DDL、open/close 和 template version 任一变化均 miss。
- SELECT/INSERT/UPDATE/DELETE 的事务、rollback、并发冲突、index、WAL/recovery、random-kill 和 `output.txt` 测试全绿。
- template hit + JIT miss、template hit + JIT hit、template miss + JIT hit、双 miss 四种组合均能独立正确工作。

### Phase 11：StatementTemplate/JIT 联动与渐进生产启用（M-L）

实施项：

1. 让完整模板直接携带 `CompiledStatement` descriptor/`JitCodeKey`，但每次仍独立查询并持有 `JitCodeCache` entry。
2. 依次部署 `shadow -> parser -> analyzer -> full`，每级独立观察至少一个稳定窗口；任一级 mismatch 自动降到完整流水线并熔断对应 key。
3. 根据真实 template build、lookup/bind/instantiate、compile/Kernel cycles 和 warmup 命中率校准两个缓存容量及 JIT auto 阈值。
4. 保留 `RMDB_STATEMENT_CACHE=off` 与 `RMDB_JIT=off` 两个独立 emergency disable，证明任意组合均可工作。

门禁：

- TPCC warmup 后热点结构 template hit rate 与 JIT code hit rate 分别达到 95% 以上，测量期没有持续建模/编译风暴。
- `full` 有效热命中样本中 Parser/Analyzer/Planner 调用数均为零；整体 flamegraph 中三者残余样本可由 miss、失效或 non-cacheable 语句逐一解释。
- 相对 Phase 0 分别报告 Lexer/lookup+bind、Parser、Analyzer、Planner、Executor 的 cycles/transaction；不能只报告占比下降而隐藏总 CPU 上升。
- 并发吞吐不因 template cache 全局锁、LRU touch 或建模 follower 等待而退化；不满足时保持较低模式而不是跳过门禁。

### Phase 12：全量验证、调参与最终启用（M-L）

1. 运行本文测试矩阵，修复所有差分和竞态。
2. 以固定 seed、交替 A/B 顺序做至少五轮 Release TPCC，至少比较 `off/off`、`auto/off`、`off/full`、`auto/full` 四组，分离前端缓存与 JIT 收益。
3. 根据真实 compile/build time、Kernel cycles、lookup/bind/instantiate 和 warmup 命中率校准阈值。
4. JIT 首次合入建议默认 `off`；StatementTemplate 首次合入建议默认 `shadow`。CI 覆盖所有模式，经过稳定验证后再分别考虑 `auto`/`full`。
5. 验证两个 emergency disable 均无需修改 SQL、重建数据库或改变输出格式即可生效。

## 16. 测试矩阵

### 16.1 单元与差分测试

| 类别 | 必测内容 |
| --- | --- |
| `StatementShapeKey` | 空白/literal 不同同 key；identifier/token/order/op/punctuation/template/catalog/planner epoch 不同异 key；digest collision 后比较 canonical bytes。 |
| Owned token/lexical binding | identifier/string 生命周期、EOF/error、一元负号与算术减号、INT 边界/溢出、FLOAT、BOOL、转义/未闭合/最大字符串。 |
| Parsed skeleton | fresh AST 所有权、全部 statement node clone/instantiate、literal display/error，与旧 Parser AST 结构差分。 |
| Semantic blueprint | alias/ambiguous column、type/cast/len/DATETIME、JOIN/aggregate/group/HAVING/order/LIMIT/UNION、SELECT/DML 参数目的地。 |
| Physical blueprint | scan/index/join/order/aggregate/LIMIT/UNION/DML 节点、planner knob、fresh Plan/Executor state，与旧 Planner canonical plan 差分。 |
| Bypass counters | `parser` 命中仅 Parser=0；`analyzer` 命中 Parser/Analyzer=0；`full` 命中 Parser/Analyzer/Planner=0；miss 均回完整流水线。 |
| `JitCodeKey` | literal 不同同 key；type/op/offset/len/plan/ABI/CPU/epoch 不同异 key；canonical collision 二次比较。 |
| IR verifier | 越界 offset、错误 len/type、坏 parameter index、未初始化结果、超大 program。 |
| Numeric predicate | INT/FLOAT/混合类型，六种 op，负数、极值、`-0.0`、NaN、Inf。 |
| Bytes predicate | 空串、前缀、等长、内嵌/缺失 NUL、最大 CHAR 长度、DATETIME。 |
| Predicate composition | 多条件 AND、第一项 false 短路、column-column、left-right join。 |
| Projection | 任意重排、重复列、连续 span、非对齐读写、各类型。 |
| Update | 五种 UpdateOp、cross numeric cast、除零和字符串 assignment。 |
| Aggregate | 空/单/多 tuple、多 aggregate、HAVING、现有 shortcut。 |
| AsmJit error | finalize/add/release 失败、unsupported arch、helper error。 |
| Template cache concurrency | single-flight、重复 follower、publish/lookup、in-flight eviction、DDL/planner race、shared template + fresh execution、shutdown。 |
| JIT cache concurrency | single-flight、publish/lookup、in-flight eviction、DDL race、shutdown。 |
| 双缓存组合 | template/code 各自 hit/miss/evict/fail 的四象限，代码 miss 不丢失前端 bypass，模板 miss 仍可命中已有代码。 |

前端每级遵循“完整 Lexer/Parser/Analyzer/Planner = 当前 bypass 结果”的差分；所有 Kernel 遵循“现有 Executor = IR interpreter = 机器码”的三方对照。随机测试使用固定 seed，失败时输出可复现 canonical token/blueprint/IR 和脱敏参数类型，但不能把原始敏感 SQL 或 literal 写入普通日志。

### 16.2 SQL 与事务测试

- JIT 关闭时，同一 `.slt` 集合分别在 `RMDB_STATEMENT_CACHE=off|shadow|parser|analyzer|full` 下运行；再用 `off/full` 分别组合 `RMDB_JIT=force` 和低阈值 `auto`，覆盖两个优化的独立性。
- 增加不带 TPCC 表名的通用用例，覆盖点查、范围查、复合过滤、Join、Projection、Insert/Delete/Update、Aggregate/Group/HAVING/Order/LIMIT/UNION、DDL 后重新建模/编译。
- 同一 shape 连续使用不同 literal，断言第二次及以后达到对应 bypass 入口计数；混入 malformed SQL、overflow、string length 和 DATETIME cast，比较错误类型和文本。
- RC、SI、SERIALIZABLE 下分别覆盖读写冲突、rollback、unique index、旧版本可见性和 SSI danger。
- 在并发 DDL、template eviction、code eviction 和 single-flight 建模/编译期间重复执行热点 shape，验证没有旧布局、UAF、死锁或共享执行状态。
- 对所有模式捕获 `data_send` 和 `output.txt`，执行 byte-for-byte 比较；不只比较行值。
- crash recovery 和 random kill 在 Update Kernel/完整模板合入后都必须运行，证明生成代码和 Planner bypass 均未绕过 WAL/undo。

建议命令（实现后）：

```bash
make build
./build/bin/test/jit_test
./build/bin/test/statement_template_test
RMDB_JIT=off ./build/bin/test/execution_test
RMDB_JIT=force ./build/bin/test/execution_test
RMDB_STATEMENT_CACHE=off RMDB_JIT=off ./build/bin/test/e2e_test
RMDB_STATEMENT_CACHE=shadow RMDB_JIT=off ./build/bin/test/e2e_test
RMDB_STATEMENT_CACHE=parser RMDB_JIT=off ./build/bin/test/e2e_test
RMDB_STATEMENT_CACHE=analyzer RMDB_JIT=off ./build/bin/test/e2e_test
RMDB_STATEMENT_CACHE=full RMDB_JIT=off ./build/bin/test/e2e_test
RMDB_STATEMENT_CACHE=full RMDB_JIT=force ./build/bin/test/e2e_test
make test
make benchmark-random-kill
```

### 16.3 Sanitizer/工具约束

- C++ wrapper、owned token、template bind/instantiate、IR 和两个 cache 路径跑 ASan/UBSan/TSan；生成机器码本身通常不会被编译器 sanitizer 插桩，因此不能用 sanitizer 代替 verifier 和 guard tests。
- Debug 构建启用 AsmJit instruction/intermediate validation。
- Linux x86-64 CI 至少保留一个真实执行 JIT 的 job；其他架构必须验证 graceful fallback。

## 17. 性能评估方案

### 17.1 A/B 控制变量

- 先 `make release`，A/B 使用同一个二进制，只改变 `RMDB_JIT` 和 `RMDB_STATEMENT_CACHE`。
- 固定 CSV、warehouses、workers、isolation、cache 容量、planner knob、run seed、warmup 和 measure。
- 每轮使用全新数据库，A/B 运行顺序交替，至少五轮；报告中位数和离散度，不挑最好一轮。
- warmup 必须足够创建完整模板并触发 auto 编译；测量结果同时报告 warmup/测量期 template build、compile 数和两个 hit rate。
- 至少比较 `JIT/cache = off/off, auto/off, off/full, auto/full`，分别量化 Executor JIT、前端 bypass 和组合收益。
- 分别测 1/8/16 worker，避免单线程收益被两个全局 cache 锁抵消。

示例：

```bash
make release
RMDB_JIT=off RMDB_STATEMENT_CACHE=off make benchmark TPCC_WORKERS=16 TPCC_WARMUP=30 TPCC_MEASURE=120 TPCC_ROUNDS=5
RMDB_JIT=auto RMDB_STATEMENT_CACHE=off make benchmark TPCC_WORKERS=16 TPCC_WARMUP=30 TPCC_MEASURE=120 TPCC_ROUNDS=5
RMDB_JIT=off RMDB_STATEMENT_CACHE=full make benchmark TPCC_WORKERS=16 TPCC_WARMUP=30 TPCC_MEASURE=120 TPCC_ROUNDS=5
RMDB_JIT=auto RMDB_STATEMENT_CACHE=full make benchmark TPCC_WORKERS=16 TPCC_WARMUP=30 TPCC_MEASURE=120 TPCC_ROUNDS=5
```

### 17.2 指标

- 主指标：median tpmC、总 TPM、五类事务 TPM、commit/abort rate。
- 延迟：每类事务 p50/p95/p99，特别观察首次模板构建/热点编译附近的 tail latency。
- CPU：cycles/transaction、instructions/transaction、IPC、branch misses、cache misses。
- 前端：Lexer/normalize、template lookup、bind、instantiate、Parser、Analyzer、Planner 各自 call count、总 ns 和 sampled cycles；各 bypass 计数与 fallback reason。
- Template：observed/eligible/ready shapes、build/publish/mismatch/evict、各 readiness hit/miss、hit rate、owned bytes、single-flight follower 和 generation/knob invalidation。
- JIT：observed shapes、eligible/queued/compiled/failed/evicted、cache hit、compile ns、code bytes、fallback reason。
- Executor：每类 Kernel call count、解释/JIT ns、fusion 命中、shadow mismatch。
- 正确性：benchmark consistency、kill-9 recovery、`output.txt` compatibility。

### 17.3 单一结果文档与产物清理

`JIT_PERFORMANCE_RESULTS.md` 中每个 `run_id` 至少包含以下固定字段：

- **可复现上下文：** timestamp、commit/dirty 标记、Phase、build/compiler/CPU、命令、数据/并发/seed、JIT/template/cache/planner 配置，以及对照 run_id。
- **测试结论：** pass/fail/interrupted、退出码、总耗时、失败用例和精简错误摘要。
- **性能与内部指标：** 第 17.2 节当轮可取得的吞吐、延迟、CPU、前端、Template、JIT、Executor 和正确性数据；未采集项写明原因。
- **判读：** 相对指定 baseline 的绝对值和百分比变化、flamegraph 观察、是否通过当前 Phase 门禁；不得只粘贴原始日志。
- **产物与清理：** 保留的小型 artifact 路径/hash、清理 manifest、cleanup 前后 bytes/可用磁盘、遗留进程/路径检查和最终 cleanup pass/fail。

wrapper 先把单轮结果写入临时 Markdown fragment，并用一次受控 append/rename 更新结果文档；确认文档可读后才删除大文件。默认删除 generated database、round temp JSON、`perf.data`、folded stack、临时 SQL/output、server log、core/trace，并轮转失败诊断；CSV 输入、源码、最终结果文档和明确选定的基线 SVG 不在 cleanup manifest 中。任何 cleanup failure 都阻止下一轮，不能以手工偶尔运行 `benchmark-clean` 代替每轮自动清理。

### 17.4 结果判读

- Kernel 快但 tpmC 不变：检查前端、network、lock/WAL 和 index profile；先完成可测的 template bypass，不盲目生成更多代码。
- `full` 热命中仍出现 Parser/Analyzer/Planner 调用：视为 bypass correctness failure，先按 digest 对照 miss/失效记录，不能用 flamegraph 比例较低掩盖。
- template build/entry 数接近 distinct literal 数：`StatementShapeKey` 参数化失败；compile 数接近 distinct literal 数：`JitCodeKey` 参数化失败。两者都必须修 key，禁止靠增大 cache 掩盖。
- template hit 高但前端 cycles 不降：检查 Lexer 重复扫描、canonical bytes 分配、binder/blueprint 深拷贝和入口计数，不把工作从一个函数搬到另一个函数后宣称成功。
- 单线程提升、并发下降：优先检查两个 cache 的 lookup/LRU/single-flight 锁竞争、shared_ptr 原子开销和编译 worker CPU 抢占。
- warmup 提升、测量抖动：检查模板/代码反复失效、容量过小、key 过度细分或 planner knob epoch 不稳定。
- abort rate 变化：视为 correctness failure，而不是性能 tradeoff。

## 18. 可观测性与输出兼容

内部统计至少包含：

- `StatementShapeKey`/`JitCodeKey` 的匿名短 digest、template readiness、Kernel kind 和 state transition。
- Lexer/normalize/lookup/build/bind/instantiate 与 Parser/Analyzer/Planner 调用/耗时，generation/planner invalidation 和 bypass/fallback reason。
- generation/compile/emission 时间、code bytes、AsmJit error string。
- 两个 cache 各自的 hit/miss/fallback/eviction/bytes 计数和 cooldown/熔断原因。
- sampled full-pipeline/bypass 或 interpreter/JIT mismatch 的 format/IR version 和最小脱敏复现信息。

默认只通过 `minilog` 或独立 benchmark metrics 文件输出，且要有采样/聚合，不能每条 TPCC SQL 打日志。以下内容明确不变：

- 客户端 `data_send` 表格和错误文本。
- `output.txt` 的 headers、分隔线、空白、record count、结束换行。
- 现有 `EXPLAIN ANALYZE` 文本。
- benchmark JSON 的既有字段语义；新增字段只能向后兼容，并由 benchmark 代码读取。

若需要把 PostgreSQL 风格的 JIT section 加入 `EXPLAIN ANALYZE`，应另开需求并先给出输出样例，不能在本实现中顺手加入。

## 19. 主要风险与缓解

| 风险 | 缓解措施 |
| --- | --- |
| 短查询编译成本超过收益 | 跨执行热度、异步编译、break-even、warmup、始终 fallback。 |
| raw SQL/literal 造成模板碎片 | 复用 Lexer 生成 token shape，literal 只进参数块；监控 entry 数与 distinct literal 的关系。 |
| token `string_view` 指向已释放请求缓冲 | `OwnedTokenStream`/模板复制 identifier/string；测试请求缓冲销毁后再 lookup/bind。 |
| 归一化误合并不同语法 | 保留全部 token 类型/顺序/运算符/标点与 canonical bytes 二次比较；shadow 对照旧 AST/Query/Plan。 |
| binder 与 Parser/Analyzer 的负号、范围、cast 或错误文本漂移 | 抽共用 helper；覆盖 unary minus/overflow/string/DATETIME；每级与完整流水线做结果和错误差分。 |
| 模板共享 mutable AST/Query/Plan/Executor 状态 | cache 只发布 immutable blueprint；每次执行 fresh instantiate，ASan/TSan 并发地址/状态隔离测试。 |
| 缓存代码捕获单次指针 | 只固化 offset/type/op；执行数据全部通过 POD frame；code review 检查绝对地址来源。 |
| DDL/planner 变化后布局或算子选择过期 | generation/knob epoch 同时进入模板与代码 key，lookup/publish/instantiate 二次检查，旧 entry 惰性失效。 |
| 淘汰与在途调用竞态 | 执行分别持 `shared_ptr<const StatementTemplate>`/`shared_ptr<const JitCode>`，最后引用后再销毁/release。 |
| 多线程重复建模/编译或锁竞争 | 两层 single-flight、follower 完整流水线 fallback、有界 worker、读路径 acquire、LRU touch 采样。 |
| 模板与机器码生命周期错误耦合 | 模板只存 descriptor/key/weak hint；每次独立 lookup。任一 cache miss/evict 都有独立正确路径。 |
| 长时间测试耗尽磁盘或遗留进程 | 每轮唯一 manifest、报告先落盘、`trap` allowlist cleanup、原始产物容量/保留数量上限、前后 `df/du` 门禁；失败立即停止后续轮次。 |
| C++ 异常穿越生成代码 | Kernel `noexcept` 返回 status，由 wrapper 在副作用前抛现有异常。 |
| 字符串/浮点语义漂移 | 首版 helper + 三方差分，明确 NUL/NaN 行为后再内联。 |
| JIT 绕过 MVCC/SSI/WAL | 只 JIT 纯计算；以源码调用顺序测试和 transaction/recovery gate 固定边界。 |
| 可执行内存被禁用或耗尽 | AsmJit error -> cooldown/fallback；entry/bytes/queue 三重上限。 |
| AsmJit/CMake 版本不兼容 | 使用并跟踪当前仓库内 clone，统一 CMake 3.24，CI 构建依赖 smoke test。 |
| x86-64 特化损害可移植性 | IR/ABI 架构中立；unsupported 全回退；AArch64 独立 gate。 |
| JIT 代码成为安全攻击面 | verifier、受限 IR、helper 白名单、W^X、无用户机器码、尺寸上限。 |
| 为追求 TPCC 数字过拟合 | 测试使用通用表/随机 IR；禁止 SQL 文本、表名、事务名分支；每个优化需有非 TPCC 微基准。 |

## 20. Code review 必查不变量

1. `OwnedTokenStream`/模板不保存指向 SQL 请求缓冲区的 `string_view`、`data()` 或本次 literal 地址。
2. `StatementShapeKey` 完整编码 token 结构和 template/catalog/planner epoch，但不含 literal 值；`JitCodeKey` 编码语义 IR/layout/ABI/CPU，二者不能混用。
3. `StatementTemplate` 发布后不可变，不持有本次 `Context/Transaction/Value`，也不共享 mutable AST/Query/Plan/Executor；每次执行 fresh instantiate。
4. lexical/semantic binder 对负号、范围、类型、长度、cast、DATETIME、显示文本和错误文本与旧 Parser/Analyzer 共用规则。
5. `full` 有效命中不调用 `parse_sql()`、`Analyze::do_analyze()` 或 `Planner::plan_query()`；miss/失效只走一次完整旧流水线。
6. DDL generation、planner knob、template format、IR/ABI version 和 CPU tier 在各自层级参与 key，并在 publish/instantiate 前复核。
7. Template/code cache 能独立 hit、miss、evict、disable；模板不得以强引用把机器码永久钉住。
8. 生成代码中没有来自本次 `Plan/Executor/Value/Context/Transaction` 的持久绝对地址，代码条目生命周期覆盖最后一次函数调用。
9. 每个 tuple/parameter/output offset 在 codegen 前经过 verifier；任意 JIT 错误都存在明确解释回退，不能改变 SQL 成败。
10. Update Kernel 或 blueprint bind 返回成功前没有持久副作用；成功后仍走原 WAL/undo/index/heap 顺序。
11. Parser 等价 lexical 错误仍发生在事务建立前，Analyzer 等价 semantic 错误仍发生在事务建立后；bypass 和 JIT Kernel 都不吞掉或移动 MVCC/SSI/lock/transaction/Portal 调用边界。
12. `output.txt`、client protocol、错误文本和 `EXPLAIN ANALYZE` 没有任何格式变化。
13. 新增分支只判断通用 token/semantic/Plan/IR 属性，不判断 TPCC SQL、表名、事务名或 benchmark 标记。
14. 每个 bypass 阶段都有完整流水线 differential/强制 fallback 测试；每个 Kernel 阶段都有解释/JIT differential/强制失败测试。
15. 每轮测试的结果文档在清理前成功落盘；cleanup 只删除该 run manifest 中的生成物，结束后无遗留进程且磁盘占用通过门禁。
16. 当前 Phase 只有在全部门禁通过后才形成一个范围纯净的 checkpoint commit；语义 diff 忽略 CRLF/LF-only 噪声，且没有行尾规范化专用提交。

## 21. 最终交付定义

只有以下证据同时具备，才可认为本计划的 JIT 与 StatementTemplate Cache 完成：

- AsmJit 已固定版本集成，支持平台执行、非支持平台无损回退。
- Predicate、Projection、Update、目标 Aggregate 的通用 IR/机器码覆盖表通过全部差分测试。
- Lexer-based 自动参数化使 TPCC 热点 `StatementShapeKey`/`JitCodeKey` 都按 shape 聚合，而不是按 literal 膨胀，且缓存不持有请求缓冲。
- 同一个 immutable `StatementTemplate` 表示已依次通过 `shadow`、Parser bypass、Analyzer bypass、Planner bypass 门禁；没有遗留三套永久前端缓存。
- `full` 热命中时 Parser/Analyzer/Planner 入口调用计数全部为零，fresh execution state 与完整流水线差分一致；TPCC 热点模板命中率达到 95% 以上。
- Template/code 两个 cache 可独立 hit/miss/evict/disable；auto hotness、异步编译、两层 single-flight、LRU/bytes cap、DDL/planner 失效和 shutdown 生命周期均有并发测试。
- `make test`、JIT 三模式、StatementTemplate 五模式、transaction/recovery/random-kill 全绿；client 输出、错误文本和 `output.txt` byte-for-byte 一致。
- Release 四组 A/B benchmark 按固定方法报告完整数据，并分别给出 Lexer/lookup+bind、Parser、Analyzer、Planner、Executor 的绝对 cycles 与 flamegraph 占比；前三段残余样本与 miss/失效完全对得上。
- 所有阶段运行都能在 `JIT_PERFORMANCE_RESULTS.md` 按 run_id 追溯；成功/失败/中断均验证“先记录、后清理”，长时间循环后无无界增长的数据库、perf、日志或临时结果。
- Phase 0-12 各有一个可追溯、范围独立且门禁全绿的 checkpoint commit；提交历史保留阶段边界，不包含无关工作区文件或 CRLF/LF-only churn。
- 达到约定的吞吐/延迟门槛，或以 profile 证据诚实说明收益被哪个非四热点环节限制；不能因占比转移就宣称优化成功。
- 文档记录已支持/未支持 template shape 与 IR 操作、默认模式/阈值、两个 cache 容量、平台范围和已知限制。

这套完成定义刻意区分三件事：“机器码能运行”只是 Phase 1，“Executor JIT 可自动安全使用”在 Phase 6 完成，“普通 SQL 热命中可跳过 Parser/Analyzer/Planner 并复用 JIT Kernel”则要到 Phase 11 才是本计划要求的最终能力。

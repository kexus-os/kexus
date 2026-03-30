# Kexus OS
## — 算子即系统的元可编程 Agent 操作系统

> **一切皆算子。** 系统内核、LLM 规划器、UI 面板、数据层——全部是通过 NCD（节点能力声明）协议自描述的 Python 算子。系统随算子演进而自动重塑，无需前端重部署。

Kexus 是一个**本地优先、数据主权**的系统。意图通过 CtxTensor（上下文张量）协议流动——执行历史以结构化张量形式累积，使复杂多跳推理链成为原生能力。

为坚持数据主权的**专业个体**（量化交易者、法律分析师、独立研究员）而建：你的意图资产与执行轨迹完全留存本地，LLM 提供商可通过配置热插拔，无需触碰代码。

---

### 核心哲学

| 概念 | 描述 |
|------|------|
| **算子即原子** | 从 HTTP 路由到 LLM 推理再到 UI 渲染，每个系统行为都是暴露 NCD 契约的 Python 算子。内核只是算子运行时沙箱。 |
| **NCD 协议** | 节点能力声明：包含 `intent`、`inputs`、`outputs`、`tags`、`safe`、`ui` 的自描述元数据，使系统可自省、自文档化。 |
| **CtxTensor 与 DataBus** | 执行上下文以版本化张量形式在 SQLite Exchange 中累积。算子可见完整上游历史，实现"意图链式"推理。通过单数制命名（`order` → `order_id`）自动推断外键。 |
| **UI 投影** | 前端组件绑定算子 NCD 的 `render` 类型。替换算子，界面自动适配。系统自我渲染。 |
| **自举** | 系统级 Flow（规划器、调度器、意图接收）本身也是算子。Go 内核仅加载第一个算子；系统通过 NCD 发现的拓扑自我配置。 |

---

### 快速开始

```bash
# 克隆至你的本地主权环境
git clone https://github.com/kexus-os/kexus.git && cd kexus

# 启动自描述内核
go run kexus.go

# 访问 http://localhost:1118
# 你所见的 UI 是当前加载算子的实时投影。
```

---

### 架构亮点

**Kexus 不是连接节点的工具，而是算子定义系统的元可编程 OS。**

- **零配置实体关联**：单数制命名约定（`order`, `order_item`），通过 `strings.TrimSuffix(field, "_id")` 自动推断主外键。无需手写 JOIN 逻辑。
- **原生人在回路 (HitL)**：一级公民支持 `__hitl_pause__` 检查点。流程状态机（`Idle` → `Running` → `Paused` → `Success`）将人工审批视为标准算子输出。
- **知识图谱编译器**：Go 内核自动从算子提取 Schema 绑定并编译 KG 拓扑，实现意图-Flow DNA 匹配（重复查询零 Token 复用）。
- **LiquidBO Schema 演进**：版本化 Schema（`meta_schema/{entity}/vN.json`），支持原子迁移与冲突检测。

---

### 开源协议

**双轨许可：开放核心 + 商业智能层**

```
版权所有 (c) 2026 Kexus OS 贡献者

Kexus OS 采用双轨许可，平衡生态开放与可持续发展：

1. 核心引擎（Go 调度器、NCD 协议、DataBus Exchange、基础算子）：
   遵循 Apache-2.0 协议
   - 包含专利保护条款
   - 允许在其上构建专有算子
   - NCD 接口作为开放标准永久不变

2. 系统算子（op_sys_planner_*、op_sys_llm_call、垂直领域包）：
   保留所有权利 / 商业许可
   - 通过 Kexus 算子市场单独分发
   - 你使用这些算子的业务逻辑仍完全属于你
```

类**"Android 模式"**：内核开放以建立信任与扩展，高级意图理解层支撑开发。无 GPL/SSPL 污染——你的数据与流程永远属于你。

---

### 路线图

- **v3.1**: 意图-Flow DNA 硬匹配（重复查询零 LLM Token 复用）  
- **v3.2**: 跨本地网络节点的分布式 CtxTensor（联邦意图链）  
- **v4.0**: 算子市场与认证垂直领域包（量化/法律/研究）



# Kexus OS
## — The Operator-Native, Self-Bootstrapping Agent Operating System

> **Everything is an Operator.** The system kernel, the LLM planner, the UI panels, and the data layer—all are operators declaring their capabilities via the NCD (Node Capability Declaration) protocol. The OS reshapes itself as operators evolve, requiring zero frontend redeployment.

Kexus is a **local-first, data-sovereign** orchestration system where intent flows through a CtxTensor (Context Tensor) protocol—accumulating execution history as structured data, enabling complex multi-hop reasoning chains without manual plumbing.

Built for professionals (quant traders, legal analysts, researchers) who demand that their intent assets and execution traces remain exclusively on their hardware, while LLM providers remain hot-swappable via configuration, not code changes.

---

### Core Philosophy

| Concept | Description |
|---------|-------------|
| **Operator as the Atom** | Every system behavior—from HTTP routing to LLM inference to UI rendering—is a Python operator exposing its contract via NCD. The kernel is merely the runtime sandbox. |
| **NCD Protocol** | Node Capability Declaration: self-describing metadata (`intent`, `inputs`, `outputs`, `tags`, `safe`, `ui`) that makes the system introspectable and self-documenting. |
| **CtxTensor & DataBus** | Execution contexts accumulate as versioned tensors in a SQLite Exchange. Operators see the full upstream history, enabling "chain-of-intent" reasoning. Foreign keys are auto-inferred via single-naming convention (`order` → `order_id`). |
| **UI Projection** | Frontend widgets bind to operator NCD `render` types. Replace an operator, the interface adapts automatically. The system renders itself. |
| **Self-Bootstrapping** | System-level flows (planner, scheduler, intent intake) are themselves operators. The Go kernel only loads the first operator; the system configures itself via NCD-discovered topology. |

---

### Quick Start

```bash
# Clone to your local sovereign environment
git clone https://github.com/kexus-os/kexus.git && cd kexus

# Launch the self-describing kernel
go run kexus.go

# Access the system at http://localhost:1118
# The UI you see is a live projection of the currently loaded operators.
```

---

### Architecture Highlights

- **Zero-Config Entity Relations**: Single-naming convention (`order`, `order_item`) with automatic foreign-key inference via `strings.TrimSuffix(field, "_id")`. No manual JOIN logic.
- **Human-in-the-Loop Native**: First-class `__hitl_pause__` checkpoints. The flow state machine (`Idle` → `Running` → `Paused` → `Success`) treats human approval as a standard operator output.
- **Knowledge Graph Compiler**: Go kernel automatically extracts schema bindings from operators and compiles KG topology, enabling intent-to-flow DNA matching (zero-token reuse for recurring patterns).
- **LiquidBO Schema Evolution**: Versioned schemas (`meta_schema/{entity}/vN.json`) with atomic migration and conflict detection.

---

### License

**Dual-Licensed: Open Core with Commercial Intelligence Layer**

```text
Copyright (c) 2026 Kexus OS Contributors

Kexus OS is dual-licensed to balance ecosystem trust and sustainable development:

1. Core Kernel (Go scheduler, NCD protocol, DataBus Exchange, builtin operators):
   Licensed under Apache-2.0
   - Patent protection included
   - You may build proprietary operators on top
   - The NCD interface is an open standard forever

2. System Operators (op_sys_planner_*, op_sys_llm_call, vertical domain packs):
   All Rights Reserved / Commercial License
   - These are distributed separately via the Kexus Operator Market
   - Your business logic using these operators remains yours
```

This creates an **"Android-like"** model: the kernel is open for trust and extension, while the advanced intent-understanding layers sustain development. No GPL/SSPL contamination—your data and your flows remain exclusively yours.

---

### Roadmap

- **v3.1**: Intent-Flow DNA hard-matching (zero-LLM-token reuse for recurring queries)  
- **v3.2**: Distributed CtxTensor across local network nodes (federated intent chains)  
- **v4.0**: Operator Market with verified vertical packs (Quant/Legal/Research)  

---

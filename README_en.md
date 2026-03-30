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

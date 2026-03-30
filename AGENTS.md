# Kexus OS v3.0 - AI Agent Guide

## Project Overview

**Kexus OS** is a hybrid Go/Python workflow orchestration system designed for AI-augmented business operations. It combines a Go-based HTTP server core with Python "operators" (executable nodes) to create flexible, flow-based automation pipelines.

The system supports:
- Flow-based execution with visual node composition
- Human-in-the-Loop (HitL) checkpoints for manual approval
- Scheduled task execution
- Real-time logging via Server-Sent Events (SSE)
- LLM integration (DeepSeek API) for AI-driven planning

## Technology Stack

| Layer | Technology |
|-------|------------|
| Backend Core | Go 1.21+ |
| Operators | Python 3.14 |
| Frontend | Vue.js 3.5, Tailwind CSS, CodeMirror 5 |
| Data Storage | JSON files + SQLite (DataBus Exchange) |
| AI Engine | DeepSeek API (OpenAI-compatible) |

## Project Structure

```
D:\_kexus/
├── kexus.go              # Main Go server - HTTP API, SSE, scheduler
├── kexus_state.json      # Runtime state (flows, ops, schedules)
├── liquid_bo.py          # Liquid Business Object engine (data layer)
├── .env                  # Environment configuration
├── init.txt              # Setup commands reference
│
├── op_*.py               # Python operators (19 files)
├── op_*.html             # Operator UI components
├── index.html            # Main Vue.js application
├── op_admin.html         # Operator debug & NCD editor
├── data/                 # Data storage directory
│   ├── *.json           # Business data (orders, products, etc.)
│   └── .exchange/       # SQLite DataBus Exchange
├── vendor/              # Cached frontend dependencies
│   ├── vue.js
│   ├── tailwind.js
│   └── codemirror.*
└── kexus_env/           # Python virtual environment
```

## Core Concepts

### Operators (算子)

Operators are Python scripts prefixed with `op_` that perform specific tasks. Each operator declares metadata via a global `NCD` dictionary:

```python
NCD = {
    "intent": "Human-readable description",
    "inputs": ["STDIN: JSON schema description"],
    "outputs": ["STDOUT: JSON schema description"],
    "tags": ["category", "labels"],
    "safe": True,           # True = idempotent (parallel safe)
    "ui": "op_example.html", # Optional UI component
    "requires": [],         # pip dependencies
    "on_error": "abort",    # abort/skip/retry:N/fallback:op_id
}
```

**Key Operator Types:**
- **Data Fetchers**: `op_fetch_*` - Query business data via LiquidBO
- **Planner**: `op_planner_*` - LLM-powered planning and execution
- **UI**: `op_flow_compositor`, `op_intent_intake` - Interface components
- **Init**: `op_init_db` - Data initialization with schema generation

### Flows

Flows are executable pipelines consisting of connected nodes. Each node references an operator and provides input parameters.

Flow states: `Idle` → `Running` → (`Success` | `Failed` | `Paused`)

### LiquidBO (Business Object Engine)

`liquid_bo.py` provides the data abstraction layer:
- Schema versioning (`meta_schema/<entity>/vN.json`)
- SQLite DataBus Exchange for large data passing
- JSON-based entity storage in `data/`

## System Initialization Flow

Kexus v3.0 采用简化的初始化流程，BO (Business Object) 创建和 KG (Knowledge Graph) 编译已分离：

```
┌─────────────┐     ┌──────────────┐     ┌─────────────────┐
│ op_init_db  │────▶│ meta_schema/ │────▶│ op_gen_fetchers │
│ (数据+Schema)│     │   (v1.json)  │     │ (生成fetch算子)  │
└─────────────┘     └──────────────┘     └─────────────────┘
                                                  │
                                                  ▼
┌─────────────────────────────────────────────────────────┐
│                    Go 引擎自动完成                        │
│  扫描算子 ──▶ 提取 Schema 绑定 ──▶ 编译 KG 拓扑           │
│  (自动触发)    (extractSchemaBindings)  (KG Compiler)    │
└─────────────────────────────────────────────────────────┘
```

### 初始化步骤

1. **首次启动或重置数据**：
   ```batch
   # 运行 op_init_db 生成测试数据和 Schema
   python op_init_db.py
   
   # 运行 op_gen_fetchers 生成数据探针算子
   python op_gen_fetchers.py
   ```

2. **启动服务器**：
   ```batch
   go run kexus.go
   ```
   服务器会自动：
   - 扫描所有算子
   - 提取 Schema 绑定（`_SCHEMA_ENTITY` 等）
   - 编译 KG 拓扑（异步，带 debounce）

3. **在 UI 中扫描算子**：
   - 打开 http://localhost:1118
   - 点击「🔍 扫描算子」按钮
   - 系统会自动触发 KG 重新编译

### 命名约定 (v3.0)

**单数制命名**：
- 表名：`order`, `order_item`, `product`, `partner`, `inventory`, `stock_move`
- 外键：`order_id`, `product_id`（`{单数表名}_id`）
- Schema 目录：`meta_schema/{entity}/vN.json`
- 数据文件：`data/{entity}.json`
- Fetcher 算子：`op_fetch_{entity}.py`

**外键推断**：Go 引擎通过 `strings.TrimSuffix(field, "_id")` 直接获取实体名，无需复数转换。

## Build & Run Commands

### Initial Setup

```batch
# Create Python virtual environment (Python 3.14 required)
py -3.14 -m venv kexus_env

# Install dependencies
pip install pandas numpy sqlalchemy psycopg2-binary requests openpyxl tabulate markdown openai TA-Lib

# Initialize Go module
go mod init kexus
go get github.com/rs/cors
```

### Development

```batch
# Activate environment and run
_env.bat          # Activates Python venv
_run.bat          # Runs: go run kexus.go

# Or manually
go run kexus.go   # Server starts on :1118
```

### Build

```batch
go build kexus.go    # Produces kexus.exe
```

## Configuration

### Environment Variables (.env)

```ini
# Core paths (REQUIRED)
KEXUS_PROJECT_DIR=D:\_kexus
KEXUS_PYTHON_EXEC=D:\_kexus\kexus_env\Scripts\python.exe

# LLM Configuration
LLM_API_KEY=sk-...
LLM_BASE_URL=https://api.deepseek.com
LLM_MODEL=deepseek-chat
```

### Operator Registration

Operators are auto-discovered via `/api/op/scan` endpoint. The Go scanner:
1. Finds all `op_*.py` files
2. Parses `NCD` dictionary using AST
3. Registers valid operators in `kexus_state.json`

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Main UI (index.html) |
| `/api/state` | GET | Get all flows and operators |
| `/api/flow/create` | POST | Create new flow |
| `/api/flow/update` | POST | Update flow nodes/metadata |
| `/api/flow/delete` | DELETE | Delete flow (if not locked) |
| `/api/flow/execute` | POST | Execute flow by ID |
| `/api/flow/resume` | POST | Resume paused flow (HitL) |
| `/api/flow/abort` | POST | Abort running flow |
| `/api/op/scan` | POST | Scan and register operators |
| `/api/op/update` | POST | Update operator metadata |
| `/api/op/delete` | DELETE | Remove operator registration |
| `/api/op/source` | GET/PUT | Read/Write operator source |
| `/api/op/run` | POST | Execute single operator |
| `/op-ui` | GET | Get operator UI by ID |
| `/op_admin` | GET | Operator debug & NCD editor |
| `/stream` | GET | SSE log stream |

## Development Guidelines

### Creating a New Operator

1. Create `op_<name>.py` with required structure:

```python
import sys
import json

NCD = {
    "intent": "What this operator does",
    "inputs": ["STDIN: {...}"],
    "outputs": ["STDOUT: {...}"],
    "tags": ["demo"],
    "safe": True,  # or False for non-idempotent
    "ui": "op_<name>.html",  # optional
    "requires": [],  # pip packages
    "on_error": "abort",
}

# Metadata dump mode (required for registration)
if len(sys.argv) == 3 and sys.argv[1] == "--dump-meta":
    with open(sys.argv[2], "w", encoding="utf-8") as f:
        json.dump(NCD, f, ensure_ascii=False)
    sys.exit(0)

def run() -> None:
    # Read input from stdin
    inputs = json.loads(sys.stdin.read())
    
    # Your logic here
    result = {"status": "success", "data": ...}
    
    # Write JSON to stdout
    sys.stdout.write(json.dumps(result, ensure_ascii=False))

if __name__ == "__main__":
    run()
```

2. (Optional) Create `op_<name>.html` for UI

3. **Debug & Edit NCD**: Access `/op_admin?id=op_<name>` for:
   - **Visual NCD Editor**: Edit intent, inputs/outputs, tags, safe flag, error handling
   - **Icon & Name Editor**: Select from 250+ categorized emojis (系统符文、宇宙空间、认知推演等13个分类)
   - **Real-time UI Preview**: Adjustable viewport sizes (mobile/tablet/desktop)
   - **Live NCD Preview**: JSON format preview with syntax highlighting
   - One-click save to persist changes to `kexus_state.json`

4. Click "🔍 扫描算子" in UI or call `/api/op/scan`

### Data Access Patterns

**Using LiquidBO for entity queries:**

```python
from liquid_bo import LiquidBO

# Fetch all records (use singular entity name)
data = LiquidBO.fetch_all("order")

# Apply filters (date range, scalar filters)
filtered = LiquidBO.apply_filters(data, inputs, entity_name="order")

# Write to exchange (for large data)
pointer = LiquidBO.write_exchange("op_name", data)
sys.stdout.write(json.dumps(pointer, ensure_ascii=False))
```

### Human-in-the-Loop (HitL)

To implement a pause checkpoint:

```python
# In operator, return special marker
result = {
    "__hitl_pause__": True,
    "message": "Waiting for approval",
    "data": {...}  # Data shown to user
}
```

The flow will pause. User can review in UI and:
- Resume: Flow continues with modified data
- Abort: Flow fails

### Icon Selector (Emoji Picker)

Both `index.html` and `op_admin.html` provide a rich emoji selector for choosing operator/flow icons:

**13 Categories (250+ icons):**
| Category | Example Icons |
|----------|---------------|
| 🔱 系统符文 | 🔱 ⚜️ ⚙️ 🔮 ✦ ◈ |
| 🌌 宇宙空间 | 🌌 🪐 🚀 🛸 ⭐ 🌙 |
| ✨ 核心机制 | ✨ 💎 🎯 ⚡ 🔥 🧊 |
| 🧠 认知推演 | 🧠 🤖 💡 🔍 🎲 ♟️ |
| 📡 网络通信 | 📡 🔗 📶 🌐 📡 📞 |
| 💾 数据存储 | 💾 📁 📂 📋 📊 📈 |
| ⚙️ 执行动作 | ⚙️ 🔧 🔨 🛠️ ⚔️ 🚀 |
| 💰 商业金融 | 💰 💎 🏆 💹 💳 ⚖️ |
| 🛡️ 安全控制 | 🛡️ 🔒 🔓 🚨 ⚠️ 🛑 |
| 🎭 界面展示 | 🎭 🖥️ 📱 💻 📷 🎬 |
| 📜 文档报告 | 📜 📄 📑 📅 📝 📰 |
| 🎮 游戏娱乐 | 🎮 🎯 🎲 🎰 🎱 🎸 |
| 🏭 工业制造 | 🏭 🏗 🏠 🏢 🏦 🏰 |

## Testing

### Operator Self-Test

Operators can implement `--self-test` mode:

```python
if len(sys.argv) >= 2 and sys.argv[1] == "--self-test":
    from liquid_bo import SelfTestRunner, NcdValidator
    t = SelfTestRunner("op_name")
    
    # Validate NCD
    is_valid, errs = NcdValidator.validate_ncd(NCD)
    t.assert_custom(is_valid, "ncd_valid", "; ".join(errs))
    
    # Run functional tests
    t.assert_equal(lambda: run_test(), expected_output)
    t.report()
```

### Manual Testing

1. Start server: `go run kexus.go`
2. Open http://localhost:1118
3. Create flow with your operator
4. Execute and monitor logs

## Security Considerations

1. **Path Traversal**: Operator source access validates paths (`filepath.Base(cmd) != cmd`)
2. **File Extensions**: Only `.py` and `.html` files allowed for source operations
3. **Locked Resources**: Flows/Operators marked `locked: true` cannot be deleted
4. **Private Resources**: `private: true` hides from planner/metadata views
5. **Concurrency**: Non-safe operators (`safe: false`) use mutex locks

## Deployment

The system is designed for single-machine deployment:

1. Build: `go build kexus.go`
2. Ensure `.env` points to correct paths
3. Run executable: `kexus.exe`
4. Access via http://localhost:1118

**Note**: The scheduler runs in-process. For production scheduling, consider external cron/scheduler calling the HTTP API.

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Python operators fail | Check `KEXUS_PYTHON_EXEC` in `.env` |
| Vendor files missing | Server auto-downloads on startup, check `vendor/` |
| Operator not appearing | Run "扫描算子" - check Python syntax in NCD |
| Flow stuck in Running | Restart server (resets to Failed state) |
| LLM errors | Verify `LLM_API_KEY` and network connectivity |
| op_admin 黑屏/报错 | Check browser console for Vue syntax errors; ensure emoji characters render correctly |
| op_admin 图标不显示 | Windows系统需确保支持彩色emoji字体 (Segoe UI Emoji) |

## Language Notes

- All comments and documentation are in **Chinese (Simplified)**
- UI labels use emoji + Chinese text
- Code identifiers use English
- Error messages are primarily Chinese

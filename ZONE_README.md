# Kexus OS v3.1 - 域管理 (Zone Management)

## 概述

Kexus OS 现在支持多域隔离，每个域是一个独立的 Kexus OS 实例，拥有独立的配置、算子、Flow 和数据。

## 目录结构

```
./
├── zone/                   # 域根目录
│   ├── _yunyao/           # 域实例（以下划线开头）
│   │   ├── .env          # 域专属环境配置
│   │   ├── kexus_state.json
│   │   ├── index.html
│   │   ├── data/
│   │   ├── meta_schema/
│   │   └── ...
│   ├── _stockk/           # 另一个域
│   └── index.html         # 域管理器首页
├── .current_zone          # 当前激活域标记文件
└── ...
```

## 访问方式

1. **域管理器**: http://localhost:1118/zone/
   - 查看所有可用域
   - 切换当前域
   - 创建新域
   - 查看域统计信息

2. **直接进入域**: http://localhost:1118/zone/{zone_id}/
   - 例如: /zone/_yunyao/

3. **API 接口**:
   - GET /api/zone/list - 获取域列表
   - GET /api/zone/current - 获取当前域
   - POST /api/zone/switch - 切换域
   - POST /api/zone/create - 创建新域

## 创建新域

通过域管理器界面或 API 创建：

```bash
curl -X POST http://localhost:1118/api/zone/create \
  -H "Content-Type: application/json" \
  -d '{"id": "_production", "name": "生产环境", "icon": "🚀"}'
```

创建后需要重启服务器才能切换到新域。

## 切换域

1. 在域管理器中点击"切换"按钮
2. 重启服务器
3. 系统会自动加载新域的配置

切换只是修改标记文件 `.current_zone`，实际切换需要重启服务器才能完全生效。

## 域隔离

每个域完全独立：
- ✅ 独立的 .env 配置
- ✅ 独立的算子 (op_*.py)
- ✅ 独立的 Flow 定义 (kexus_state.json)
- ✅ 独立的 Flow 调试数据缓存 (kexus_flow_cache.json) ⭐ v3.1 新增
- ✅ 独立的数据 (data/)
- ✅ 独立的 Schema (meta_schema/)
- ✅ 独立的日志和备份

### v3.1 存储分离

**kexus_state.json** - 只包含配置信息：
- 算子列表 (ops)
- Flow 配置 (flows.nodes, flows.meta 等)
- 调度设置

**kexus_flow_cache.json** - 包含运行时调试数据：
- Flow 执行输出 (outputs)
- 节点状态 (node_statuses)

这种分离保持配置文件的干净，便于版本控制，同时允许调试数据独立增长。

## 注意事项

1. 域ID必须以下划线开头（如：`_yunyao`）
2. 切换域后需要重启服务器
3. 共享 `vendor/` 目录（前端库）
4. 域管理器本身位于 `zone/index.html`

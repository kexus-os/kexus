// ==============================================================================
// 🚀 Kexus OS v3.0 — Go Data Engine HTTP API
// 提供内部 API 供 Python 算子访问 Go 数据引擎
// ==============================================================================

package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"kexus/engine"
	_ "modernc.org/sqlite"
)

// 全局引擎实例（在main中初始化）
var dataEngine *engine.SchemaEngine

// initDataEngine 初始化数据引擎
func initDataEngine() {
	schemaDir := filepath.Join(ProjectDir, "meta_schema")
	dataEngine = engine.NewSchemaEngine(schemaDir)
	
	// v3.1: 设置全局数据目录（支持域隔离）
	engine.DataDir = filepath.Join(ProjectDir, "data")
}

// apiEngineQuery 数据查询 API
// POST /api/engine/query
// Body: {"entity": "orders", "filters": {"order_type": "sale", "__outputs__": {...}}}
// Response: {"data": [...], "count": 123} 或 Exchange 指针
func apiEngineQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Entity  string                 `json:"entity"`
		Filters map[string]interface{} `json:"filters"`
		OpID    string                 `json:"op_id,omitempty"` // 用于 Exchange 写入
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if req.Entity == "" {
		http.Error(w, "entity is required", http.StatusBadRequest)
		return
	}

	// 调用 Go 引擎执行查询
	results, err := engine.ApplyFilters(req.Entity, req.Filters, nil)
	if err != nil {
		http.Error(w, fmt.Sprintf("查询失败: %v", err), http.StatusInternalServerError)
		return
	}

	// 如果指定了 op_id，写入 Exchange 并返回指针
	if req.OpID != "" {
		pointer := engine.WriteExchange(req.OpID, results)
		json.NewEncoder(w).Encode(pointer)
		return
	}

	// 直接返回数据
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status": "success",
		"entity": req.Entity,
		"data":   results,
		"count":  len(results),
	})
}

// apiEngineSchema Schema 信息 API
// GET /api/engine/schema?entity=orders
// Response: EntitySchema JSON
func apiEngineSchema(w http.ResponseWriter, r *http.Request) {
	entity := r.URL.Query().Get("entity")
	if entity == "" {
		http.Error(w, "entity parameter required", http.StatusBadRequest)
		return
	}

	if dataEngine == nil {
		http.Error(w, "Engine not initialized", http.StatusInternalServerError)
		return
	}

	schema, err := dataEngine.LoadLatestSchema(entity)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if schema == nil {
		http.Error(w, "Schema not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	json.NewEncoder(w).Encode(schema)
}

// apiEngineExchangeWrite Exchange 写入 API
// POST /api/engine/exchange/write
// Body: {"op_id": "op_fetch_orders", "data": [...]}
// Response: {"__kexus_exchange__": "pointer_id"}
func apiEngineExchangeWrite(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		OpID string                   `json:"op_id"`
		Data []map[string]interface{} `json:"data"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	pointer := engine.WriteExchange(req.OpID, req.Data)
	json.NewEncoder(w).Encode(pointer)
}

// apiEngineExchangeRead Exchange 读取 API
// POST /api/engine/exchange/read
// Body: {"__kexus_exchange__": "pointer_id"}
// Response: {"data": [...]}
func apiEngineExchangeRead(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Pointer string `json:"__kexus_exchange__"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	data, err := engine.ReadExchange(req.Pointer)
	if err != nil {
		http.Error(w, fmt.Sprintf("读取失败: %v", err), http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"data": data,
	})
}



// apiEngineEvolve Schema 进化 API
// POST /api/engine/evolve
// Body: {"entity": "orders", "properties": {...}, "primary_key": "id"}
// Response: {"version": "v2", "created": true}
func apiEngineEvolve(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Entity     string                           `json:"entity"`
		Properties map[string]engine.SchemaProperty `json:"properties"`
		PrimaryKey string                           `json:"primary_key"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if req.Entity == "" {
		http.Error(w, "entity is required", http.StatusBadRequest)
		return
	}

	if dataEngine == nil {
		http.Error(w, "Engine not initialized", http.StatusInternalServerError)
		return
	}

	// 调用 Evolve
	version, err := dataEngine.Evolve(req.Entity, req.Properties, req.PrimaryKey)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// 获取最新版本号来判断是否创建了新版本
	latestVer, _ := dataEngine.GetLatestVersion(req.Entity)
	isNew := version == fmt.Sprintf("v%d", latestVer)

	// v3.0: Schema 进化后触发 KG 自动重编译
	if isNew {
		engine.RequestKGRecompile()
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"version": version,
		"created": isNew,
		"entity":  req.Entity,
	})
}

// apiEngineKGCompile 知识图谱编译 API
// POST /api/engine/kg/compile
// Response: {"nodes_compiled": 10, "edges_linked": 15, "prompt_topology": "..."}
func apiEngineKGCompile(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// 1. 扫描所有算子
	ops, err := scanOperators(ProjectDir, PythonExec)
	if err != nil {
		http.Error(w, fmt.Sprintf("扫描算子失败: %v", err), http.StatusInternalServerError)
		return
	}
	
	// 调试：输出算子 Schema 绑定
	fetchOps := 0
	for _, op := range ops {
		if op.SchemaEntity != "" {
			fetchOps++
		}
	}
	fmt.Printf("[KG Compile] 扫描到 %d 个算子，其中 %d 个有 Schema 绑定\n", len(ops), fetchOps)

	// 2. 提取 Schema 绑定信息（从算子源码）
	opsWithBindings := extractSchemaBindings(ops, ProjectDir)
	
	// 调试：确认转换后
	boundOps := 0
	for _, op := range opsWithBindings {
		if op.SchemaEntity != "" {
			boundOps++
		}
	}
	fmt.Printf("[KG Compile] 转换后 %d 个算子有 Schema 绑定\n", boundOps)

	// 3. 创建 KG 编译器
	kgc := engine.NewKGCompiler(ProjectDir, opsWithBindings)

	// 4. 编译知识图谱
	result, err := kgc.Compile()
	if err != nil {
		http.Error(w, fmt.Sprintf("编译失败: %v", err), http.StatusInternalServerError)
		return
	}

	// 构建与 Python 版本兼容的响应格式
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status": "success",
		"data": map[string]interface{}{
			"nodes_compiled":  result.NodesCompiled,
			"edges_linked":    result.EdgesLinked,
			"prompt_topology": result.PromptTopology,
			"graph_db_path":   filepath.Join(ProjectDir, "data", ".exchange", "kg_topology.db"),
			"nodes_list":      result.Nodes,
			"edges_list":      result.Edges,
		},
	})
}

// apiEngineKGQuery 查询已有知识图谱
// POST /api/engine/kg/query
func apiEngineKGQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	dbPath := filepath.Join(ProjectDir, "data", ".exchange", "kg_topology.db")
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status": "empty",
			"nodes":  []interface{}{},
			"edges":  []interface{}{},
		})
		return
	}

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer db.Close()

	rows, err := db.Query("SELECT id, type, properties FROM kg_nodes")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var nodes []map[string]interface{}
	for rows.Next() {
		var id, nType, props string
		if err := rows.Scan(&id, &nType, &props); err != nil {
			continue
		}
		var propsMap map[string]interface{}
		json.Unmarshal([]byte(props), &propsMap)
		nodes = append(nodes, map[string]interface{}{
			"id":         id,
			"type":       nType,
			"properties": propsMap,
		})
	}

	edgeRows, err := db.Query("SELECT source, target, relation FROM kg_edges")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer edgeRows.Close()

	var edges []map[string]interface{}
	for edgeRows.Next() {
		var source, target, relation string
		if err := edgeRows.Scan(&source, &target, &relation); err != nil {
			continue
		}
		edges = append(edges, map[string]interface{}{
			"source":   source,
			"target":   target,
			"relation": relation,
		})
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"status": "success",
		"data": map[string]interface{}{
			"nodes_compiled": len(nodes),
			"edges_linked":   len(edges),
			"nodes_list":     nodes,
			"edges_list":     edges,
		},
	})
}

// extractSchemaBindings 从算子源码提取 Schema 绑定信息
func extractSchemaBindings(ops []engine.OpInfo, projectDir string) []engine.OpInfo {
	// 改进的正则：允许行首有空格（缩进）
	reEntity := regexp.MustCompile(`(?m)^\s*_SCHEMA_ENTITY\s*=\s*["']([^"']+)["']`)
	reVersion := regexp.MustCompile(`(?m)^\s*_SCHEMA_VERSION\s*=\s*["']([^"']+)["']`)
	reHash := regexp.MustCompile(`(?m)^\s*_SCHEMA_HASH\s*=\s*["']([^"']+)["']`)

	// 转换为 engine.OpInfo
	result := make([]engine.OpInfo, 0, len(ops))
	
	for _, op := range ops {
		// 从文件提取 Schema 绑定
		schemaEntity := op.SchemaEntity
		schemaVersion := op.SchemaVersion
		schemaHash := op.SchemaHash
		
		// 如果 scanOperators 没有提取到，从文件再试一次
		if schemaEntity == "" {
			opPath := filepath.Join(projectDir, op.Cmd)
			content, err := os.ReadFile(opPath)
			if err == nil {
				text := string(content)
				if m := reEntity.FindStringSubmatch(text); m != nil {
					schemaEntity = m[1]
				}
				if m := reVersion.FindStringSubmatch(text); m != nil {
					schemaVersion = m[1]
				}
				if m := reHash.FindStringSubmatch(text); m != nil {
					schemaHash = m[1]
				}
			}
		}
		
		// 有 entity 但没有 version，默认 v1
		if schemaEntity != "" && schemaVersion == "" {
			schemaVersion = "v1"
		}
		
		// 转换 Meta
		var metaMap map[string]interface{}
		if op.Meta != nil {
			metaMap = op.Meta
		}
		
		result = append(result, engine.OpInfo{
			ID:             op.ID,
			Cmd:            op.Cmd,
			Intent:         op.Intent,
			Tags:           op.Tags,
			Meta:           metaMap,
			SchemaEntity:   schemaEntity,
			SchemaVersion:  schemaVersion,
			SchemaHash:     schemaHash,
			ProducesTopics: op.ProducesTopics,
			ConsumesTopics: op.ConsumesTopics,
		})
	}

	return result
}

// apiEngineInit 系统初始化 API
// POST /api/engine/init
// 替代 op_init_db + op_init_bo + op_gen_fetchers + op_kg_init 的串行调用
func apiEngineInit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Action string `json:"action"` // "full" | "schema" | "kg" | "fetchers" | "exchange"
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		req.Action = "full" // 默认全量初始化
	}

	results := make(map[string]interface{})

	// 1. Schema 目录初始化
	if req.Action == "full" || req.Action == "schema" {
		schemaDir := filepath.Join(ProjectDir, "meta_schema")
		if err := ensureDir(schemaDir); err != nil {
			results["schema"] = map[string]interface{}{"error": err.Error()}
		} else {
			results["schema"] = map[string]interface{}{"status": "ok"}
		}
	}

	// 2. Exchange 目录初始化
	if req.Action == "full" || req.Action == "exchange" {
		exchangeDir := filepath.Join(ProjectDir, "data", ".exchange")
		if err := ensureDir(exchangeDir); err != nil {
			results["exchange"] = map[string]interface{}{"error": err.Error()}
		} else {
			results["exchange"] = map[string]interface{}{"status": "ok"}
		}
	}

	// 3. KG 编译
	if req.Action == "full" || req.Action == "kg" {
		ops, err := scanOperators(ProjectDir, PythonExec)
		if err != nil {
			results["kg"] = map[string]interface{}{
				"status": "error",
				"error":  fmt.Sprintf("扫描算子失败: %v", err),
			}
		} else {
			opsWithBindings := extractSchemaBindings(ops, ProjectDir)
			kgc := engine.NewKGCompiler(ProjectDir, opsWithBindings)
			kgResult, err := kgc.Compile()
			if err != nil {
				results["kg"] = map[string]interface{}{
					"status": "error",
					"error":  fmt.Sprintf("编译失败: %v", err),
				}
			} else {
				results["kg"] = map[string]interface{}{
					"status":         "ok",
					"nodes_compiled": kgResult.NodesCompiled,
					"edges_linked":   kgResult.EdgesLinked,
				}
			}
		}
	}

	// 4. Fetcher 代码生成检查
	if req.Action == "full" || req.Action == "fetchers" {
		results["fetchers"] = map[string]interface{}{
			"status":  "ok",
			"message": "Go engine mode, Python fetchers not required",
		}
	}

	// v3.0: 初始化完成后触发 KG 自动重编译
	engine.RequestKGRecompile()

	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":  "success",
		"action":  req.Action,
		"results": results,
	})
}

// apiEngineInsert 数据插入 API
// POST /api/engine/insert
// Body: {"entity": "orders", "records": [{...}, {...}]}
// Response: {"inserted": 2, "skipped": 0}
func apiEngineInsert(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Entity       string                   `json:"entity"`
		Records      []map[string]interface{} `json:"records"`
		SkipExisting bool                     `json:"skip_existing,omitempty"` // 是否跳过已存在的记录（基于 id 或唯一键）
		UniqueKey    string                   `json:"unique_key,omitempty"`    // 用于判断重复的唯一键，默认 "id"
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if req.Entity == "" {
		http.Error(w, "entity is required", http.StatusBadRequest)
		return
	}
	if len(req.Records) == 0 {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status":   "success",
			"inserted": 0,
			"skipped":  0,
		})
		return
	}

	// 数据文件路径
	dataFile := filepath.Join(ProjectDir, "data", req.Entity+".json")

	// 读取现有数据
	var existing []map[string]interface{}
	if data, err := os.ReadFile(dataFile); err == nil {
		json.Unmarshal(data, &existing)
	}

	// 构建唯一键集合
	uniqueKey := req.UniqueKey
	if uniqueKey == "" {
		uniqueKey = "id"
	}
	existingKeys := make(map[string]bool)
	for _, rec := range existing {
		if key, ok := rec[uniqueKey].(string); ok {
			existingKeys[key] = true
		}
	}

	// 插入新记录
	inserted := 0
	skipped := 0
	for _, rec := range req.Records {
		key, hasKey := rec[uniqueKey].(string)
		if req.SkipExisting && hasKey && existingKeys[key] {
			skipped++
			continue
		}
		existing = append(existing, rec)
		if hasKey {
			existingKeys[key] = true
		}
		inserted++
	}

	// 写回文件
	dataDir := filepath.Join(ProjectDir, "data")
	os.MkdirAll(dataDir, 0755)
	if err := os.WriteFile(dataFile, func() []byte {
		b, _ := json.MarshalIndent(existing, "", "  ")
		return b
	}(), 0644); err != nil {
		http.Error(w, fmt.Sprintf("写入数据失败: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":   "success",
		"entity":   req.Entity,
		"inserted": inserted,
		"skipped":  skipped,
		"total":    len(existing),
	})
}

// ensureDir 确保目录存在
func ensureDir(dir string) error {
	return os.MkdirAll(dir, 0755)
}

// isFetcherOp 判断是否为 fetcher 算子
func isFetcherOp(opID string) bool {
	return strings.HasPrefix(opID, "op_fetch_")
}

// extractEntityFromFetcher 从 fetcher op_id 提取实体名
// 例如：op_fetch_orders -> orders
func extractEntityFromFetcher(opID string) string {
	if !isFetcherOp(opID) {
		return ""
	}
	return strings.TrimPrefix(opID, "op_fetch_")
}

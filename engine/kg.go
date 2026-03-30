// ==============================================================================
// 🕸️ Knowledge Graph 模块
// 负责：知识图谱编译、拓扑构建、LLM 友好序列化
// 对应原 op_kg_init.py 的功能
// ==============================================================================

package engine

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

// KGNode 知识图谱节点
type KGNode struct {
	ID         string                 `json:"id"`
	Type       string                 `json:"type"` // ENTITY or OPERATOR
	Properties map[string]interface{} `json:"properties"`
}

// KGEdge 知识图谱边
type KGEdge struct {
	Source string `json:"source"`
	Target string `json:"target"`
	Rel    string `json:"relation"`
}

// KGResult 知识图谱编译结果
type KGResult struct {
	Nodes          []KGNode `json:"nodes"`
	Edges          []KGEdge `json:"edges"`
	PromptTopology string   `json:"prompt_topology"`
	NodesCompiled  int      `json:"nodes_compiled"`
	EdgesLinked    int      `json:"edges_linked"`
}

// OpInfo 算子信息（从 kexus.go 导入）
type OpInfo struct {
	ID             string                 `json:"id"`
	Cmd            string                 `json:"cmd"`
	Intent         string                 `json:"intent"`
	Tags           []string               `json:"tags"`
	Meta           map[string]interface{} `json:"meta"`
	SchemaEntity   string                 `json:"schema_entity,omitempty"`
	SchemaVersion  string                 `json:"schema_version,omitempty"`
	SchemaHash     string                 `json:"schema_hash,omitempty"`
	ProducesTopics []string               `json:"produces_topics,omitempty"`
	ConsumesTopics []string               `json:"consumes_topics,omitempty"`
}

// KGCompiler 知识图谱编译器
type KGCompiler struct {
	ProjectDir string
	SchemaDir  string
	DBPath     string
	Ops        []OpInfo // 算子列表（由外部传入）
}

// NewKGCompiler 创建 KG 编译器
func NewKGCompiler(projectDir string, ops []OpInfo) *KGCompiler {
	return &KGCompiler{
		ProjectDir: projectDir,
		SchemaDir:  filepath.Join(projectDir, "meta_schema"),
		DBPath:     filepath.Join(projectDir, "data", ".exchange", "kg_topology.db"),
		Ops:        ops,
	}
}

// Compile 编译知识图谱
func (kgc *KGCompiler) Compile() (*KGResult, error) {
	// 初始化数据库
	db, err := kgc.initDB()
	if err != nil {
		return nil, err
	}
	defer db.Close()

	// 清空旧数据
	if err := kgc.clearDB(db); err != nil {
		return nil, err
	}

	// 1. 编译所有节点（BO + OP）
	nodes, err := kgc.compileNodes()
	if err != nil {
		return nil, err
	}

	// 2. 过滤：BO 只保留最新版本
	latestNodes := kgc.filterBOLatest(nodes)

	// 3. 用过滤后的节点编译边
	edges := kgc.compileEdges(latestNodes)

	// 持久化到数据库
	if err := kgc.persistToDB(db, latestNodes, edges); err != nil {
		return nil, err
	}

	// 生成 prompt_topology
	promptTopology := SerializeTopologyForPrompt(latestNodes, edges)

	// 转换为值类型（而非指针）
	resultNodes := make([]KGNode, 0, len(latestNodes))
	resultEdges := make([]KGEdge, 0, len(edges))
	for _, n := range latestNodes {
		resultNodes = append(resultNodes, *n)
	}
	for _, e := range edges {
		resultEdges = append(resultEdges, *e)
	}

	return &KGResult{
		Nodes:          resultNodes,
		Edges:          resultEdges,
		PromptTopology: promptTopology,
		NodesCompiled:  len(latestNodes),
		EdgesLinked:    len(edges),
	}, nil
}

// initDB 初始化数据库
func (kgc *KGCompiler) initDB() (*sql.DB, error) {
	if err := os.MkdirAll(filepath.Dir(kgc.DBPath), 0755); err != nil {
		return nil, err
	}

	db, err := sql.Open("sqlite", kgc.DBPath)
	if err != nil {
		return nil, err
	}

	if _, err := db.Exec("PRAGMA journal_mode = WAL"); err != nil {
		return nil, err
	}

	return db, nil
}

// clearDB 清空数据库
func (kgc *KGCompiler) clearDB(db *sql.DB) error {
	queries := []string{
		`DROP TABLE IF EXISTS kg_edges`,
		`DROP TABLE IF EXISTS kg_nodes`,
		`DROP TABLE IF EXISTS vec_nodes`,
		`CREATE TABLE kg_nodes (
			id TEXT PRIMARY KEY,
			type TEXT NOT NULL CHECK(type IN ('ENTITY', 'OPERATOR')),
			properties TEXT NOT NULL
		)`,
		`CREATE TABLE kg_edges (
			source TEXT NOT NULL,
			target TEXT NOT NULL,
			relation TEXT NOT NULL,
			topic TEXT,
			PRIMARY KEY (source, target, relation)
		)`,
		`CREATE INDEX idx_edges_source ON kg_edges(source)`,
		`CREATE INDEX idx_edges_target ON kg_edges(target)`,
		`CREATE INDEX idx_edges_relation ON kg_edges(relation)`,
	}

	for _, q := range queries {
		if _, err := db.Exec(q); err != nil {
			return err
		}
	}
	return nil
}

// compileNodes 编译所有节点
func (kgc *KGCompiler) compileNodes() ([]*KGNode, error) {
	nodes := make([]*KGNode, 0)

	// 1. 编译 BO 节点
	boNodes, err := kgc.compileBONodes()
	if err != nil {
		return nil, err
	}
	nodes = append(nodes, boNodes...)

	// 2. 编译 OP 节点（使用传入的 OpInfo）
	opNodes := kgc.compileOPNodes()
	nodes = append(nodes, opNodes...)

	return nodes, nil
}

// compileBONodes 编译业务对象节点
func (kgc *KGCompiler) compileBONodes() ([]*KGNode, error) {
	nodes := make([]*KGNode, 0)

	entries, err := os.ReadDir(kgc.SchemaDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nodes, nil
		}
		return nil, err
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		entityName := entry.Name()
		versions, err := kgc.getSchemaVersions(entityName)
		if err != nil {
			continue
		}

		for _, ver := range versions {
			nodeID := fmt.Sprintf("bo_%s_%s", entityName, ver)
			props := map[string]interface{}{
				"entity":  entityName,
				"version": ver,
			}

			// 读取 schema 字段
			schema, err := kgc.loadSchema(entityName, ver)
			if err == nil && schema != nil {
				props["schema_fields"] = kgc.formatSchemaFields(schema)
			}

			nodes = append(nodes, &KGNode{
				ID:         nodeID,
				Type:       "ENTITY",
				Properties: props,
			})
		}
	}

	return nodes, nil
}

// compileOPNodes 编译算子节点（使用传入的 OpInfo）
func (kgc *KGCompiler) compileOPNodes() []*KGNode {
	nodes := make([]*KGNode, 0)

	for _, op := range kgc.Ops {
		// 跳过自身
		if op.ID == "op_kg_init" {
			continue
		}

		nodes = append(nodes, &KGNode{
			ID:   op.ID,
			Type: "OPERATOR",
			Properties: map[string]interface{}{
				"intent": op.Intent,
				"tags":   op.Tags,
			},
		})
	}

	return nodes
}

// compileEdges 编译边关系（使用已过滤的节点）
func (kgc *KGCompiler) compileEdges(nodes []*KGNode) []*KGEdge {
	edges := make([]*KGEdge, 0)

	// 构建节点查找表和实体名到节点ID的映射
	nodeMap := make(map[string]*KGNode)
	entityNodeID := make(map[string]string) // entity -> node_id（节点已是最新版本）
	for _, n := range nodes {
		nodeMap[n.ID] = n
		if n.Type == "ENTITY" {
			entity := n.Properties["entity"].(string)
			entityNodeID[entity] = n.ID
		}
	}

	// 1. Schema 绑定边（算子 -> BO）
	for _, op := range kgc.Ops {
		if op.SchemaEntity == "" {
			continue
		}
		
		// 直接查找该实体对应的 BO 节点（已是最新版本）
		targetNodeID, ok := entityNodeID[op.SchemaEntity]
		if !ok {
			continue // 该实体的 BO 节点不存在
		}
		
		// READS 边：BO -> OP（算子读取实体数据）
		edges = append(edges, &KGEdge{
			Source: targetNodeID,
			Target: op.ID,
			Rel:    "READS",
		})

		// WRITES 边：OP -> BO（fetch 算子默认有 WRITES 语义）
		if strings.HasPrefix(op.ID, "op_fetch_") {
			edges = append(edges, &KGEdge{
				Source: op.ID,
				Target: targetNodeID,
				Rel:    "WRITES",
			})
		}
	}

	// 2. 外键关系边（BO -> BO）
	// 命名约定：表名使用单数形式，外键字段为 {entity}_id
	for _, node := range nodes {
		if node.Type != "ENTITY" {
			continue
		}

		entity := node.Properties["entity"].(string)
		version := node.Properties["version"].(string)
		schema, _ := kgc.loadSchema(entity, version)

		if schema != nil {
			for field := range schema.Properties {
				if !strings.HasSuffix(field, "_id") {
					continue
				}

				// 提取可能的实体名（单数制）
				// 支持带前缀的外键，如 source_channel_partner_id → channel_partner
				baseName := strings.TrimSuffix(field, "_id")

				// 查找匹配的实体节点（检查后缀匹配）
				var targetID string
				if id, ok := entityNodeID[baseName]; ok {
					// 直接匹配，如 product_id → product
					targetID = id
				} else {
					// 检查后缀匹配，如 source_channel_partner_id 匹配 channel_partner
					for entityName, nodeID := range entityNodeID {
						suffix := entityName + "_id"
						if strings.HasSuffix(field, suffix) {
							targetID = nodeID
							break
						}
					}
				}

				if targetID != "" && targetID != node.ID {
					edges = append(edges, &KGEdge{
						Source: node.ID,
						Target: targetID,
						Rel:    fmt.Sprintf("FK:%s", field),
					})
				}
			}
		}
	}

	// 3. Topic 契约边（OP -> OP）
	producers := make(map[string][]string) // topic -> []op_id
	consumers := make(map[string][]string) // topic -> []op_id

	for _, op := range kgc.Ops {
		for _, topic := range op.ProducesTopics {
			producers[topic] = append(producers[topic], op.ID)
		}
		for _, topic := range op.ConsumesTopics {
			consumers[topic] = append(consumers[topic], op.ID)
		}
	}

	for topic, prods := range producers {
		cons := consumers[topic]
		for _, p := range prods {
			for _, c := range cons {
				if p != c {
					edges = append(edges, &KGEdge{
						Source: p,
						Target: c,
						Rel:    fmt.Sprintf("TOPIC_LINK:%s", topic),
					})
				}
			}
		}
	}

	return edges
}

// filterBOLatest 过滤 BO 只保留最新版本，OP 全部保留
func (kgc *KGCompiler) filterBOLatest(nodes []*KGNode) []*KGNode {
	// 找出每个实体的最新版本（数字比较）
	latestVersions := make(map[string]int) // entity -> version number
	for _, n := range nodes {
		if n.Type != "ENTITY" {
			continue
		}
		entity := n.Properties["entity"].(string)
		versionStr := strings.TrimPrefix(n.Properties["version"].(string), "v")
		version, _ := strconv.Atoi(versionStr)

		if latest, ok := latestVersions[entity]; !ok || version > latest {
			latestVersions[entity] = version
		}
	}

	// 过滤节点
	keptNodes := make([]*KGNode, 0)
	for _, n := range nodes {
		if n.Type == "OPERATOR" {
			// OP 全部保留
			keptNodes = append(keptNodes, n)
		} else {
			// BO 只保留最新版本
			entity := n.Properties["entity"].(string)
			versionStr := strings.TrimPrefix(n.Properties["version"].(string), "v")
			version, _ := strconv.Atoi(versionStr)
			if latestVersions[entity] == version {
				keptNodes = append(keptNodes, n)
			}
		}
	}

	return keptNodes
}

// filterLatestVersions 过滤只保留最新版本（旧版本，保留用于兼容）
func (kgc *KGCompiler) filterLatestVersions(nodes []*KGNode, edges []*KGEdge) ([]*KGNode, []*KGEdge) {
	latestNodes := kgc.filterBOLatest(nodes)
	
	// 构建保留的节点 ID 集合
	keptIDs := make(map[string]bool)
	for _, n := range latestNodes {
		keptIDs[n.ID] = true
	}

	// 过滤边（只保留两端都在保留节点中的边）
	keptEdges := make([]*KGEdge, 0)
	for _, e := range edges {
		if keptIDs[e.Source] && keptIDs[e.Target] {
			keptEdges = append(keptEdges, e)
		}
	}

	return latestNodes, keptEdges
}

// persistToDB 持久化到数据库
func (kgc *KGCompiler) persistToDB(db *sql.DB, nodes []*KGNode, edges []*KGEdge) error {
	// 插入节点
	nodeStmt, err := db.Prepare("INSERT INTO kg_nodes (id, type, properties) VALUES (?, ?, ?)")
	if err != nil {
		return err
	}
	defer nodeStmt.Close()

	for _, n := range nodes {
		props, _ := json.Marshal(n.Properties)
		if _, err := nodeStmt.Exec(n.ID, n.Type, string(props)); err != nil {
			return err
		}
	}

	// 插入边
	edgeStmt, err := db.Prepare("INSERT INTO kg_edges (source, target, relation) VALUES (?, ?, ?)")
	if err != nil {
		return err
	}
	defer edgeStmt.Close()

	for _, e := range edges {
		if _, err := edgeStmt.Exec(e.Source, e.Target, e.Rel); err != nil {
			return err
		}
	}

	return nil
}

// SerializeTopologyForPrompt 序列化为 LLM 友好的拓扑文本
func SerializeTopologyForPrompt(nodes []*KGNode, edges []*KGEdge) string {
	lines := []string{}
	lines = append(lines, "=== SYSTEM TOPOLOGY (Knowledge Graph) ===")

	// 实体节点
	entities := make([]*KGNode, 0)
	for _, n := range nodes {
		if n.Type == "ENTITY" {
			entities = append(entities, n)
		}
	}
	if len(entities) > 0 {
		lines = append(lines, "")
		lines = append(lines, "[Entities]")
		for _, n := range entities {
			entity := n.Properties["entity"].(string)
			version := n.Properties["version"].(string)
			fields := ""
			if f, ok := n.Properties["schema_fields"].(string); ok {
				fields = f
			}
			line := fmt.Sprintf("  %s (%s %s)", n.ID, entity, version)
			if fields != "" {
				line += fmt.Sprintf("  fields: %s", fields)
			}
			lines = append(lines, line)
		}
	}

	// 算子节点
	operators := make([]*KGNode, 0)
	for _, n := range nodes {
		if n.Type == "OPERATOR" {
			operators = append(operators, n)
		}
	}
	if len(operators) > 0 {
		lines = append(lines, "")
		lines = append(lines, "[Operators]")
		for _, n := range operators {
			intent := ""
			if i, ok := n.Properties["intent"].(string); ok {
				intent = i
			}
			tags := []string{}
			if t, ok := n.Properties["tags"].([]interface{}); ok {
				for _, tag := range t {
					if s, ok := tag.(string); ok {
						tags = append(tags, s)
					}
				}
			}
			tagStr := ""
			if len(tags) > 0 {
				tagStr = fmt.Sprintf("  tags: [%s]", strings.Join(tags, ", "))
			}
			lines = append(lines, fmt.Sprintf("  %s: %s%s", n.ID, intent, tagStr))
		}
	}

	// 边关系
	if len(edges) > 0 {
		lines = append(lines, "")
		lines = append(lines, "[Relations]")
		for _, e := range edges {
			lines = append(lines, fmt.Sprintf("  %s --[%s]--> %s", e.Source, e.Rel, e.Target))
		}
	}

	lines = append(lines, "")
	lines = append(lines, "=== END TOPOLOGY ===")
	return strings.Join(lines, "\n")
}

// QueryExistingGraph 查询已有图谱（供外部调用）
func QueryExistingGraph(dbPath string) (*KGResult, error) {
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("图谱数据库不存在")
	}

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	// 读取节点
	rows, err := db.Query("SELECT id, type, properties FROM kg_nodes")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	allNodes := make(map[string]*KGNode)
	var nodesList []*KGNode
	for rows.Next() {
		var id, nType, propsStr string
		if err := rows.Scan(&id, &nType, &propsStr); err != nil {
			continue
		}
		var props map[string]interface{}
		json.Unmarshal([]byte(propsStr), &props)
		node := &KGNode{ID: id, Type: nType, Properties: props}
		allNodes[id] = node
		nodesList = append(nodesList, node)
	}

	// 版本过滤：BO 只保留最新版本
	entityVersions := make(map[string]string) // entity -> latest_version_node_id
	for id, node := range allNodes {
		if node.Type != "ENTITY" {
			continue
		}
		entity := node.Properties["entity"].(string)
		if existing, ok := entityVersions[entity]; !ok {
			entityVersions[entity] = id
		} else {
			// 比较版本（转换为数字比较，避免字符串比较问题 v10 < v9）
			existingVerStr := allNodes[existing].Properties["version"].(string)
			currentVerStr := node.Properties["version"].(string)
			existingVer, _ := strconv.Atoi(strings.TrimPrefix(existingVerStr, "v"))
			currentVer, _ := strconv.Atoi(strings.TrimPrefix(currentVerStr, "v"))
			if currentVer > existingVer {
				entityVersions[entity] = id
			}
		}
	}

	keptIDs := make(map[string]bool)
	var filteredNodes []*KGNode
	for _, node := range nodesList {
		if node.Type == "OPERATOR" {
			keptIDs[node.ID] = true
			filteredNodes = append(filteredNodes, node)
		} else {
			entity := node.Properties["entity"].(string)
			if entityVersions[entity] == node.ID {
				keptIDs[node.ID] = true
				filteredNodes = append(filteredNodes, node)
			}
		}
	}

	// 读取边
	edgeRows, err := db.Query("SELECT source, target, relation FROM kg_edges")
	if err != nil {
		return nil, err
	}
	defer edgeRows.Close()

	var filteredEdges []*KGEdge
	for edgeRows.Next() {
		var source, target, relation string
		if err := edgeRows.Scan(&source, &target, &relation); err != nil {
			continue
		}
		if keptIDs[source] && keptIDs[target] {
			filteredEdges = append(filteredEdges, &KGEdge{Source: source, Target: target, Rel: relation})
		}
	}

	// 生成 prompt_topology
	promptTopology := SerializeTopologyForPrompt(filteredNodes, filteredEdges)

	// 转换为值类型
	resultNodes := make([]KGNode, 0, len(filteredNodes))
	resultEdges := make([]KGEdge, 0, len(filteredEdges))
	for _, n := range filteredNodes {
		resultNodes = append(resultNodes, *n)
	}
	for _, e := range filteredEdges {
		resultEdges = append(resultEdges, *e)
	}

	return &KGResult{
		Nodes:          resultNodes,
		Edges:          resultEdges,
		PromptTopology: promptTopology,
		NodesCompiled:  len(filteredNodes),
		EdgesLinked:    len(filteredEdges),
	}, nil
}

// 辅助函数

func (kgc *KGCompiler) getSchemaVersions(entityName string) ([]string, error) {
	entityPath := filepath.Join(kgc.SchemaDir, entityName)
	entries, err := os.ReadDir(entityPath)
	if err != nil {
		return nil, err
	}

	versions := make([]string, 0)
	re := regexp.MustCompile(`^v(\d+)\.json$`)
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		matches := re.FindStringSubmatch(e.Name())
		if len(matches) == 2 {
			versions = append(versions, fmt.Sprintf("v%s", matches[1]))
		}
	}

	sort.Strings(versions)
	return versions, nil
}

func (kgc *KGCompiler) loadSchema(entityName, version string) (*EntitySchema, error) {
	schemaPath := filepath.Join(kgc.SchemaDir, entityName, version+".json")
	data, err := os.ReadFile(schemaPath)
	if err != nil {
		return nil, err
	}

	var schema EntitySchema
	if err := json.Unmarshal(data, &schema); err != nil {
		return nil, err
	}

	return &schema, nil
}

func (kgc *KGCompiler) formatSchemaFields(schema *EntitySchema) string {
	fields := make([]string, 0, len(schema.Properties))
	// 按 key 排序保证确定性
	keys := make([]string, 0, len(schema.Properties))
	for name := range schema.Properties {
		keys = append(keys, name)
	}
	sort.Strings(keys)

	for _, name := range keys {
		prop := schema.Properties[name]
		if prop.Type == "enum" && len(prop.EnumValues) > 0 {
			fields = append(fields, fmt.Sprintf("%s(枚举:[%s])", name, strings.Join(prop.EnumValues, "|")))
		} else {
			fields = append(fields, name)
		}
	}
	return strings.Join(fields, ", ")
}

// =============================================================================
// 🧠 KG 自动重编译触发器（带 debounce）
// =============================================================================

var (
	kgRecompileChan = make(chan struct{}, 1) // 带缓冲，非阻塞写入
	kgDebounceOnce  sync.Once
)

// RequestKGRecompile 请求 KG 重编译（非阻塞，可从任何 goroutine 安全调用）
func RequestKGRecompile() {
	select {
	case kgRecompileChan <- struct{}{}:
	default:
		// channel 已满，说明已经有一个待处理的请求，跳过
	}
}

// StartKGRecompileWorker 启动 KG 重编译后台 worker
// projectDir: 项目目录
// getOpsFn: 获取算子列表的回调函数
// broadcastFn: 广播日志的回调函数
// 在 main() 中调用一次
func StartKGRecompileWorker(projectDir string, getOpsFn func() []OpInfo, broadcastFn func(string)) {
	kgDebounceOnce.Do(func() {
		go func() {
			for range kgRecompileChan {
				// Debounce：等待 500ms，如果期间有新请求则重置
				time.Sleep(500 * time.Millisecond)
				// 排空 channel 中可能累积的多余请求
				for len(kgRecompileChan) > 0 {
					<-kgRecompileChan
				}

				start := time.Now()
				ops := getOpsFn()
				kgc := NewKGCompiler(projectDir, ops)
				result, err := kgc.Compile()
				if err != nil {
					broadcastFn(fmt.Sprintf("⚠️ [KG] 自动重编译失败: %v", err))
				} else {
					broadcastFn(fmt.Sprintf(
						"🧠 [KG] 自动重编译完成: %d 节点, %d 边 (%dms)",
						result.NodesCompiled,
						result.EdgesLinked,
						time.Since(start).Milliseconds(),
					))
				}
			}
		}()
	})
}

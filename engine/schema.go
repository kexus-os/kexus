// ==============================================================================
// 📐 Schema 管理模块
// 负责：Schema 加载、版本管理、类型推断、外键推断
// 对应原 liquid_bo.py 和 op_init_bo.py 的 Schema 相关功能
// ==============================================================================

package engine

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	// TYPE_SCAN_ROWS 类型推断时扫描的行数，与 Python 版保持一致
	TYPE_SCAN_ROWS = 100
	// ENUM_MAX_UNIQUE 字符串字段坍缩为 enum 的最大唯一值数量
	ENUM_MAX_UNIQUE = 15
)

// 向后兼容的类型别名（供其他文件使用）
type Schema = EntitySchema
type Property = SchemaProperty
type SchemaManager = SchemaEngine

// SchemaProperty 对应 meta_schema/*/vN.json 中每个字段的定义
type SchemaProperty struct {
	Type       string   `json:"type"`                  // "string"|"integer"|"float"|"boolean"|"enum"
	Required   bool     `json:"required"`
	Nullable   bool     `json:"nullable,omitempty"`
	EnumValues []string `json:"enum_values,omitempty"`
}

// EntitySchema 对应一个完整的 schema 版本文件
type EntitySchema struct {
	EntityName  string                    `json:"entity_name"`
	Version     string                    `json:"version"`
	PrimaryKey  string                    `json:"primary_key"`
	Timestamp   string                    `json:"timestamp"`
	Properties  map[string]SchemaProperty `json:"properties"`
	ForeignKeys map[string]ForeignKey     `json:"foreign_keys,omitempty"` // 向后兼容
}

// ForeignKey 表示外键关系（向后兼容）
type ForeignKey struct {
	Field     string `json:"field"`
	RefEntity string `json:"ref_entity"`
	RefField  string `json:"ref_field"`
}

// SchemaEngine Schema 引擎
type SchemaEngine struct {
	SchemaDir string
}

// NewSchemaEngine 创建 Schema 引擎
func NewSchemaEngine(schemaDir string) *SchemaEngine {
	return &SchemaEngine{SchemaDir: schemaDir}
}

// LoadLatestSchema 加载指定实体的最新 schema 版本
func (se *SchemaEngine) LoadLatestSchema(entityName string) (*EntitySchema, error) {
	entityPath := filepath.Join(se.SchemaDir, entityName)
	
	// 检查目录是否存在
	if _, err := os.Stat(entityPath); os.IsNotExist(err) {
		return nil, nil // 实体不存在
	}

	// 获取最新版本
	latestVer, err := se.GetLatestVersion(entityName)
	if err != nil {
		return nil, err
	}
	if latestVer == 0 {
		return nil, nil // 无版本
	}

	// 读取 schema 文件
	versionStr := fmt.Sprintf("v%d", latestVer)
	schemaPath := filepath.Join(entityPath, versionStr+".json")
	data, err := os.ReadFile(schemaPath)
	if err != nil {
		return nil, fmt.Errorf("读取 schema 文件失败: %w", err)
	}

	var schema EntitySchema
	if err := json.Unmarshal(data, &schema); err != nil {
		return nil, fmt.Errorf("解析 schema 失败: %w", err)
	}

	return &schema, nil
}

// GetLatestVersion 获取指定实体的最新版本号
func (se *SchemaEngine) GetLatestVersion(entityName string) (int, error) {
	entityPath := filepath.Join(se.SchemaDir, entityName)
	
	entries, err := os.ReadDir(entityPath)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}

	latestVer := 0
	re := regexp.MustCompile(`^v(\d+)\.json$`)

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		matches := re.FindStringSubmatch(entry.Name())
		if len(matches) == 2 {
			ver, _ := strconv.Atoi(matches[1])
			if ver > latestVer {
				latestVer = ver
			}
		}
	}

	return latestVer, nil
}

// Evolve Schema 进化：对比新旧 properties，相同则跳过，不同则写入 vN+1.json
// 返回使用的版本号（新版本或当前版本）
func (se *SchemaEngine) Evolve(entityName string, newProps map[string]SchemaProperty, primaryKey string) (string, error) {
	// 确保实体目录存在
	entityPath := filepath.Join(se.SchemaDir, entityName)
	if err := os.MkdirAll(entityPath, 0755); err != nil {
		return "", fmt.Errorf("创建实体目录失败: %w", err)
	}

	// 获取当前最新版本
	currentVer, err := se.GetLatestVersion(entityName)
	if err != nil {
		return "", err
	}

	// 如果有现有版本，比较 properties
	if currentVer > 0 {
		currentSchema, err := se.LoadLatestSchema(entityName)
		if err != nil {
			return "", err
		}

		// 如果 properties 相同，不创建新版本
		if currentSchema != nil && propertiesEqual(currentSchema.Properties, newProps) {
			return currentSchema.Version, nil
		}
	}

	// 创建新版本
	newVer := currentVer + 1
	versionStr := fmt.Sprintf("v%d", newVer)

	schema := EntitySchema{
		EntityName: entityName,
		Version:    versionStr,
		PrimaryKey: primaryKey,
		Timestamp:  time.Now().Format(time.RFC3339),
		Properties: newProps,
	}

	// 写入文件
	schemaPath := filepath.Join(entityPath, versionStr+".json")
	data, err := json.MarshalIndent(schema, "", "  ")
	if err != nil {
		return "", fmt.Errorf("序列化 schema 失败: %w", err)
	}

	if err := os.WriteFile(schemaPath, data, 0644); err != nil {
		return "", fmt.Errorf("写入 schema 文件失败: %w", err)
	}

	return versionStr, nil
}

// propertiesEqual 比较两个 properties 是否相等（与 Python 版 _properties_equal 行为一致）
func propertiesEqual(a, b map[string]SchemaProperty) bool {
	if len(a) != len(b) {
		return false
	}

	for key, propA := range a {
		propB, ok := b[key]
		if !ok {
			return false
		}
		if !propertyEqual(propA, propB) {
			return false
		}
	}

	return true
}

// propertyEqual 比较单个 property 是否相等
func propertyEqual(a, b SchemaProperty) bool {
	if a.Type != b.Type {
		return false
	}
	if a.Required != b.Required {
		return false
	}
	if a.Nullable != b.Nullable {
		return false
	}
	// 比较 enum_values
	if len(a.EnumValues) != len(b.EnumValues) {
		return false
	}
	for i, v := range a.EnumValues {
		if v != b.EnumValues[i] {
			return false
		}
	}
	return true
}

// InferProperties 类型推断：从数据样本推断 properties
// 移植 op_init_bo.py 的 infer_dominant_type 逻辑
// 注意：bool 判断必须在 int 之前（Go 的 JSON 解析会把 JSON bool 解析为 bool）
func (se *SchemaEngine) InferProperties(rows []map[string]any, scanLimit int) map[string]SchemaProperty {
	if scanLimit <= 0 {
		scanLimit = TYPE_SCAN_ROWS
	}

	// 限制扫描行数
	if len(rows) > scanLimit {
		rows = rows[:scanLimit]
	}

	// 收集每个字段的类型统计
	typeStats := make(map[string]map[string]int) // field -> type -> count
	valueSamples := make(map[string]map[string]bool) // field -> value -> exists

	for _, row := range rows {
		for field, val := range row {
			if typeStats[field] == nil {
				typeStats[field] = make(map[string]int)
				valueSamples[field] = make(map[string]bool)
			}

			typeName := InferType(val)
			typeStats[field][typeName]++

			// 收集字符串唯一值（用于 enum 判断）
			if typeName == "string" && val != nil {
				valueSamples[field][fmt.Sprintf("%v", val)] = true
			}
		}
	}

	// 构建 properties
	properties := make(map[string]SchemaProperty)
	for field, stats := range typeStats {
		// 类型后处理：如果同时存在 integer 和 float，优先选择 float
		if stats["float"] > 0 && stats["integer"] > 0 {
			stats["float"] += stats["integer"]
			delete(stats, "integer")
		}

		// 找出主导类型
		dominantType := "string"
		maxCount := 0
		for t, count := range stats {
			if count > maxCount {
				maxCount = count
				dominantType = t
			}
		}

		prop := SchemaProperty{
			Type:     dominantType,
			Required: stats["null"] == 0,
			Nullable: stats["null"] > 0,
		}

		// enum 坍缩判断：string 字段 + 非时间 + 唯一值 ≤ ENUM_MAX_UNIQUE
		if dominantType == "string" && !isTimeField(field) {
			uniqueValues := make([]string, 0, len(valueSamples[field]))
			for v := range valueSamples[field] {
				uniqueValues = append(uniqueValues, v)
			}
			if len(uniqueValues) > 0 && len(uniqueValues) <= ENUM_MAX_UNIQUE {
				prop.Type = "enum"
				sort.Strings(uniqueValues)
				prop.EnumValues = uniqueValues
			}
		}

		properties[field] = prop
	}

	return properties
}

// inferType 推断单个值的类型
// InferType 推断单个值的类型（导出供其他模块使用）
func InferType(val any) string {
	if val == nil {
		return "null"
	}

	switch v := val.(type) {
	case bool:
		return "boolean"
	case int, int8, int16, int32, int64:
		return "integer"
	case uint, uint8, uint16, uint32, uint64:
		return "integer"
	case float32, float64:
		// 判断是否为整数
		if f, ok := v.(float64); ok {
			if f == float64(int64(f)) {
				return "integer"
			}
		}
		return "float"
	case string:
		// 尝试解析为时间
		if isTimeValue(v) {
			return "datetime"
		}
		return "string"
	case []any:
		return "array"
	case map[string]any:
		return "object"
	default:
		return "string"
	}
}

// isTimeField 判断字段名是否为时间字段
func isTimeField(field string) bool {
	lower := strings.ToLower(field)
	timePatterns := []string{"time", "date", "_at", "created", "updated", "timestamp"}
	for _, p := range timePatterns {
		if strings.Contains(lower, p) {
			return true
		}
	}
	return false
}

// isTimeValue 判断字符串值是否为时间格式
func isTimeValue(val string) bool {
	// ISO8601 格式检查
	isoPatterns := []string{
		`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}`,
		`^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}`,
		`^\d{4}-\d{2}-\d{2}$`,
	}
	for _, p := range isoPatterns {
		if matched, _ := regexp.MatchString(p, val); matched {
			return true
		}
	}
	return false
}

// InferForeignKeys 外键推断：基于命名约定 {entity_name}_id 推断外键关系
// 输入：当前表 schema + 上游表名列表
// 输出：map[当前表字段名]上游表名
func (se *SchemaEngine) InferForeignKeys(schema *EntitySchema, upstreamEntities []string) map[string]string {
	fks := make(map[string]string)

	// 构建上游表名查找集（包含单数和复数形式）
	upstreamSet := make(map[string]string) // name -> canonical name
	for _, name := range upstreamEntities {
		upstreamSet[name] = name
		// 处理复数形式
		if strings.HasSuffix(name, "s") {
			singular := name[:len(name)-1]
			upstreamSet[singular] = name
		}
		// 处理单数形式
		upstreamSet[name+"s"] = name
	}

	for fieldName := range schema.Properties {
		// 检查是否以 _id 结尾
		if !strings.HasSuffix(fieldName, "_id") {
			continue
		}

		// 提取可能的实体名
		baseName := strings.TrimSuffix(fieldName, "_id")

		// 尝试匹配上游表
		if target, ok := upstreamSet[baseName]; ok {
			fks[fieldName] = target
		} else if target, ok := upstreamSet[baseName+"s"]; ok {
			// 尝试复数形式
			fks[fieldName] = target
		}
	}

	return fks
}

// ComputeHash 计算 properties 的哈希值（用于 OP 绑定校验）
func ComputePropertiesHash(props map[string]SchemaProperty) string {
	// 序列化 properties（按键排序）
	keys := make([]string, 0, len(props))
	for k := range props {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	h := sha256.New()
	for _, k := range keys {
		data, _ := json.Marshal(props[k])
		h.Write([]byte(k))
		h.Write(data)
	}

	return hex.EncodeToString(h.Sum(nil))[:16]
}

// ListEntities 列出所有实体名称
func (se *SchemaEngine) ListEntities() ([]string, error) {
	entries, err := os.ReadDir(se.SchemaDir)
	if err != nil {
		if os.IsNotExist(err) {
			return []string{}, nil
		}
		return nil, err
	}

	entities := make([]string, 0)
	for _, entry := range entries {
		if entry.IsDir() {
			entities = append(entities, entry.Name())
		}
	}
	sort.Strings(entities)
	return entities, nil
}

// ToPromptFormat 将 Schema 转换为 Prompt 友好的格式
func (es *EntitySchema) ToPromptFormat() string {
	fields := make([]string, 0, len(es.Properties))
	for name, prop := range es.Properties {
		if prop.Type == "enum" {
			fields = append(fields, fmt.Sprintf("%s(枚举:[%s])", name, strings.Join(prop.EnumValues, "|")))
		} else {
			fields = append(fields, name)
		}
	}
	sort.Strings(fields)
	return fmt.Sprintf("- 实体: %s | 属性: %s", es.EntityName, strings.Join(fields, ", "))
}

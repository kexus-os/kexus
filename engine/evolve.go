// ==============================================================================
// 🔄 Evolve 模块
// 负责：Schema 进化、版本管理、自动升级
// 对应原 op_init_bo.py 的功能
// ==============================================================================

package engine

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// SchemaEvolver 管理 Schema 进化
type SchemaEvolver struct {
	SchemaDir string
	DataDir   string
}

// NewSchemaEvolver 创建 Schema 进化器
func NewSchemaEvolver(schemaDir, dataDir string) *SchemaEvolver {
	return &SchemaEvolver{
		SchemaDir: schemaDir,
		DataDir:   dataDir,
	}
}

// EvolveResult 进化结果
type EvolveResult struct {
	EntityName    string `json:"entity_name"`
	OldVersion    string `json:"old_version"`
	NewVersion    string `json:"new_version"`
	ChangedFields []string `json:"changed_fields"`
	RecordsMigrated int  `json:"records_migrated"`
}

// DetectChanges 检测数据变化并生成新 Schema
func (se *SchemaEvolver) DetectChanges(entityName string) (*Schema, error) {
	dataPath := filepath.Join(se.DataDir, entityName+".json")
	
	// 读取现有数据
	data, err := se.loadData(dataPath)
	if err != nil {
		return nil, err
	}

	if len(data) == 0 {
		return nil, nil // 无数据
	}

	// 推断 Schema
	inferredSchema := se.inferSchemaFromData(entityName, data)
	
	return inferredSchema, nil
}

// inferSchemaFromData 从数据中推断 Schema
func (se *SchemaEvolver) inferSchemaFromData(entityName string, data []map[string]interface{}) *Schema {
	if len(data) == 0 {
		return nil
	}

	schema := &Schema{
		EntityName:  entityName,
		Version:     "v1",
		Properties:  make(map[string]Property),
		ForeignKeys: make(map[string]ForeignKey),
	}

	// 收集所有字段及其类型
	fieldTypes := make(map[string]map[string]int)
	for _, record := range data {
		for field, value := range record {
			if fieldTypes[field] == nil {
				fieldTypes[field] = make(map[string]int)
			}
			typeName := InferType(value)
			fieldTypes[field][typeName]++
		}
	}

	// 为每个字段确定类型
	for field, types := range fieldTypes {
		// 选择出现次数最多的类型
		maxCount := 0
		mainType := "string"
		for t, count := range types {
			if count > maxCount {
				maxCount = count
				mainType = t
			}
		}

		prop := Property{Type: mainType}
		
		// 检测枚举类型（如果不同值数量少）
		if mainType == "string" {
			uniqueValues := se.collectUniqueValues(data, field)
			if len(uniqueValues) > 0 && len(uniqueValues) <= 10 {
				prop.Type = "enum"
				prop.EnumValues = uniqueValues
			}
		}

		// 检测外键（字段名以 _id 结尾）
		if strings.HasSuffix(field, "_id") {
			refEntity := field[:len(field)-3]
			schema.ForeignKeys[field] = ForeignKey{
				Field:     field,
				RefEntity: refEntity,
				RefField:  "id",
			}
		}

		schema.Properties[field] = prop
	}

	return schema
}

// collectUniqueValues 收集字段的唯一值
func (se *SchemaEvolver) collectUniqueValues(data []map[string]interface{}, field string) []string {
	seen := make(map[string]bool)
	for _, record := range data {
		if val, ok := record[field]; ok {
			str := fmt.Sprintf("%v", val)
			if str != "" {
				seen[str] = true
			}
		}
	}

	result := make([]string, 0, len(seen))
	for val := range seen {
		result = append(result, val)
	}
	return result
}

// loadData 加载数据文件
func (se *SchemaEvolver) loadData(path string) ([]map[string]interface{}, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return []map[string]interface{}{}, nil
		}
		return nil, err
	}

	var result []map[string]interface{}
	if err := json.Unmarshal(data, &result); err != nil {
		// 尝试解析为 JSONL
		return se.parseJSONL(data)
	}
	return result, nil
}

// parseJSONL 解析 JSONL 格式
func (se *SchemaEvolver) parseJSONL(data []byte) ([]map[string]interface{}, error) {
	var result []map[string]interface{}
	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var record map[string]interface{}
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			continue
		}
		result = append(result, record)
	}
	return result, nil
}

// SaveSchema 保存 Schema 到文件
func (se *SchemaEvolver) SaveSchema(schema *Schema) error {
	entityDir := filepath.Join(se.SchemaDir, schema.EntityName)
	if err := os.MkdirAll(entityDir, 0755); err != nil {
		return err
	}

	schemaPath := filepath.Join(entityDir, schema.Version+".json")
	data, err := json.MarshalIndent(schema, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(schemaPath, data, 0644)
}

// ComputeHash 计算 Schema 哈希
func ComputeHash(props map[string]Property) string {
	// 将属性排序后序列化
	h := sha256.New()
	encoder := json.NewEncoder(h)
	encoder.Encode(props)
	return hex.EncodeToString(h.Sum(nil))[:16]
}

// detectType 检测值的类型// CompareSchemas 比较两个 Schema 的差异
func CompareSchemas(old, new *Schema) []string {
	changes := make([]string, 0)

	// 检查新增字段
	for field := range new.Properties {
		if _, ok := old.Properties[field]; !ok {
			changes = append(changes, "+"+field)
		}
	}

	// 检查删除字段
	for field := range old.Properties {
		if _, ok := new.Properties[field]; !ok {
			changes = append(changes, "-"+field)
		}
	}

	// 检查类型变化
	for field, oldProp := range old.Properties {
		if newProp, ok := new.Properties[field]; ok {
			if oldProp.Type != newProp.Type {
				changes = append(changes, "~"+field+":"+oldProp.Type+">"+newProp.Type)
			}
		}
	}

	return changes
}

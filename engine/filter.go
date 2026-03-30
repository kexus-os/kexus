// ==============================================================================
// 🔍 Filter 模块
// 负责：谓词下推、标量过滤、Hash Semi-Join、外键约束构建
// ==============================================================================

package engine

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// DataDir 全局数据目录配置（由 main 包在初始化时设置）
var DataDir = "data"

// FilterLogger 过滤日志回调函数类型
type FilterLogger func(msg string)

// Predicate 表示过滤条件
type Predicate struct {
	Field    string      // 字段名
	Operator string      // 操作符: eq, ne, gt, gte, lt, lte, in, contains
	Value    interface{} // 值
}

// FilterEngine 提供数据过滤能力
type FilterEngine struct {
	SchemaEng *SchemaEngine
}

// NewFilterEngine 创建过滤引擎
func NewFilterEngine(se *SchemaEngine) *FilterEngine {
	return &FilterEngine{SchemaEng: se}
}

// ApplyFilters 对实体数据应用完整的过滤逻辑（包括标量过滤和外键约束）
// 这是 Go 引擎的核心查询函数，对应 Python 的 LiquidBO.fetch_all + apply_filters
func ApplyFilters(entityName string, filters map[string]interface{}, logger ...FilterLogger) ([]map[string]interface{}, error) {
	log := func(msg string) {}
	if len(logger) > 0 && logger[0] != nil {
		log = logger[0]
	}

	log(fmt.Sprintf("🚀 [DataBus Pushdown] 算子域: %s", entityName))

	// 第一步：加载原始数据
	data, err := loadEntityData(entityName)
	if err != nil {
		return nil, fmt.Errorf("加载数据失败: %w", err)
	}
	log(fmt.Sprintf("📂 加载数据文件: %d 行", len(data)))

	// 第二步：从 filters 中提取 __outputs__（上游节点的 Exchange 指针）
	outputs, _ := filters["__outputs__"].(map[string]interface{})

	// 第三步：构建外键约束（基于上游数据）
	fkConstraints := BuildFKConstraints(entityName, outputs)
	for fkField, ids := range fkConstraints {
		log(fmt.Sprintf("🔗 注入外键约束: %s IN (%d 个值)", fkField, len(ids)))
	}

	// 第四步：提取标量过滤条件（排除 __outputs__ 键）
	scalarFilters := make(map[string]interface{})
	for k, v := range filters {
		if k != "__outputs__" {
			scalarFilters[k] = v
			log(fmt.Sprintf("🔍 标量条件: %s = %v", k, v))
		}
	}

	// 第五步：应用过滤
	var results []map[string]interface{}
	scanned := 0
	for _, row := range data {
		scanned++
		// 检查外键约束
		if !matchFKConstraints(row, fkConstraints) {
			continue
		}
		// 检查标量过滤
		if !matchScalarFilters(row, scalarFilters) {
			continue
		}
		results = append(results, row)
	}

	log(fmt.Sprintf("✂️ 扫描 %d 行，命中 %d 行", scanned, len(results)))
	return results, nil
}

// loadEntityData 加载实体数据文件
func loadEntityData(entityName string) ([]map[string]interface{}, error) {
	// 使用全局配置的 DataDir（支持域隔离）
	dataDir := DataDir
	if dataDir == "" {
		dataDir = "data"
	}

	// 优先尝试 jsonl 格式
	jsonlPath := filepath.Join(dataDir, entityName+".jsonl")
	if _, err := os.Stat(jsonlPath); err == nil {
		return loadJSONL(jsonlPath)
	}

	// 回退到 json 格式
	jsonPath := filepath.Join(dataDir, entityName+".json")
	if _, err := os.Stat(jsonPath); err == nil {
		return loadJSON(jsonPath)
	}

	// 文件不存在，返回空数据
	return []map[string]interface{}{}, nil
}

// loadJSON 加载 JSON 数组文件
func loadJSON(path string) ([]map[string]interface{}, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var result []map[string]interface{}
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, err
	}
	return result, nil
}

// loadJSONL 加载 JSONL 文件
func loadJSONL(path string) ([]map[string]interface{}, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var result []map[string]interface{}
	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var record map[string]interface{}
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			continue // 跳过解析失败的行
		}
		result = append(result, record)
	}
	return result, nil
}

// BuildFKConstraints 构建外键约束（基于上游输出数据）
// 对应 liquid_bo.py 第 254-292 行的外键推断逻辑
// 命名约定：表名使用单数形式，外键字段名为 {entity}_id
func BuildFKConstraints(currentEntity string, outputs map[string]interface{}) map[string]map[string]bool {
	result := make(map[string]map[string]bool)
	if len(outputs) == 0 {
		return result
	}

	// 当前实体的反向外键字段名（单数制）
	// 例如 currentEntity = "order_item" → revFK = "order_item_id"
	revFKField := currentEntity + "_id"

	// 加载当前实体的 schema properties（用于判断正向外键是否存在）
	schemaEng := NewSchemaEngine("meta_schema")
	schema, _ := schemaEng.LoadLatestSchema(currentEntity)
	var schemaProps map[string]SchemaProperty
	if schema != nil {
		schemaProps = schema.Properties
	}

	// 遍历上游输出
	for upOpID, upDataPtr := range outputs {
		// 解析 Exchange 指针，获取上游数据
		upData, err := ResolveExchange(upDataPtr)
		if err != nil || len(upData) == 0 {
			continue
		}

		// 从 op_id 推断上游实体名（单数制）
		// op_fetch_product → product → fk: product_id
		upEntity := strings.TrimPrefix(upOpID, "op_fetch_")
		fkField := upEntity + "_id"

		// 推断 1：正向外键（当前表有 product_id 字段，依赖上游 product 表）
		if schemaProps != nil {
			if _, exists := schemaProps[fkField]; exists {
				validIDs := make(map[string]bool)
				for _, row := range upData {
					if id, ok := row["id"]; ok {
						validIDs[fmt.Sprint(id)] = true
					}
				}
				if existing, has := result[fkField]; has {
					// 交集：多个上游约束同一个外键时取交集
					for id := range existing {
						if !validIDs[id] {
							delete(existing, id)
						}
					}
				} else {
					result[fkField] = validIDs
				}
			}
		}

		// 推断 2：反向外键（上游数据中有 order_item_id 字段，反向关联当前表）
		if len(upData) > 0 {
			// 取样检查前 min(5, len(upData)) 行，避免首行恰好缺少字段的偶发问题
			sampleSize := 5
			if len(upData) < sampleSize {
				sampleSize = len(upData)
			}
			fieldExists := false
			for i := 0; i < sampleSize; i++ {
				if _, exists := upData[i][revFKField]; exists {
					fieldExists = true
					break
				}
			}
			if fieldExists {
				validIDs := make(map[string]bool)
				for _, row := range upData {
					if id, ok := row[revFKField]; ok {
						validIDs[fmt.Sprint(id)] = true
					}
				}
				if existing, has := result["id"]; has {
					for id := range existing {
						if !validIDs[id] {
							delete(existing, id)
						}
					}
				} else {
					result["id"] = validIDs
				}
			}
		}
	}

	return result
}

// ResolveExchange 解析 Exchange 指针获取数据
// 调用 exchange.go 中的 ReadExchange 实现
func ResolveExchange(ptr interface{}) ([]map[string]interface{}, error) {
	// 如果是 map（包含 __kexus_exchange__ 键），提取指针 ID
	if ptrMap, ok := ptr.(map[string]interface{}); ok {
		if ptrID, ok := ptrMap["__kexus_exchange__"].(string); ok {
			// 调用 exchange.go 中的 ReadExchange（完整的 SQLite 实现）
			return ReadExchange(ptrID)
		}
	}
	// 如果直接是数组，直接返回
	if data, ok := ptr.([]map[string]interface{}); ok {
		return data, nil
	}
	if data, ok := ptr.([]interface{}); ok {
		result := make([]map[string]interface{}, 0, len(data))
		for _, item := range data {
			if row, ok := item.(map[string]interface{}); ok {
				result = append(result, row)
			}
		}
		return result, nil
	}
	return nil, fmt.Errorf("无法解析 Exchange 指针: %v", ptr)
}

// matchFKConstraints 检查行是否满足外键约束
func matchFKConstraints(row map[string]interface{}, constraints map[string]map[string]bool) bool {
	for field, validIDs := range constraints {
		rowValue, exists := row[field]
		if !exists {
			return false
		}
		if !validIDs[fmt.Sprint(rowValue)] {
			return false
		}
	}
	return true
}

// matchScalarFilters 检查行是否满足标量过滤条件
// 完整移植 Python 的四种过滤模式
func matchScalarFilters(row map[string]interface{}, filters map[string]interface{}) bool {
	for field, value := range filters {
		rowValue, exists := row[field]
		if !exists {
			continue // Python 版对不存在的字段跳过而非拒绝
		}

		fieldLower := strings.ToLower(field)
		isTime := strings.Contains(fieldLower, "time") ||
			strings.Contains(fieldLower, "date") ||
			strings.Contains(fieldLower, "_at")
		isStrict := strings.HasSuffix(fieldLower, "_type") ||
			strings.HasSuffix(fieldLower, "_status") ||
			strings.HasSuffix(fieldLower, "_id") ||
			fieldLower == "id" ||
			strings.HasSuffix(fieldLower, "_no") ||
			strings.HasSuffix(fieldLower, "_code") ||
			strings.HasSuffix(fieldLower, "_key")

		// 规则 1：时间字段 + [start, end] 数组
		if isTime {
			if arr, ok := value.([]interface{}); ok && len(arr) == 2 {
				sv := fmt.Sprint(rowValue)
				s := fmt.Sprint(arr[0])
				e := fmt.Sprint(arr[1])
				if s != "" && s != "<nil>" && sv < s {
					return false
				}
				if e != "" && e != "<nil>" && sv > e {
					return false
				}
				continue
			}
			// 非数组：精确匹配
			if fmt.Sprint(rowValue) != fmt.Sprint(value) {
				return false
			}
			continue
		}

		// 规则 2：数值字段 + [min, max] 数组
		if rowNum, ok := toFloat64(rowValue); ok {
			if arr, ok := value.([]interface{}); ok && len(arr) == 2 {
				if arr[0] != nil && arr[0] != "" {
					if minVal, ok := toFloat64(arr[0]); ok && rowNum < minVal {
						return false
					}
				}
				if arr[1] != nil && arr[1] != "" {
					if maxVal, ok := toFloat64(arr[1]); ok && rowNum > maxVal {
						return false
					}
				}
				continue
			}
		}

		// 规则 3：严格字段 → 精确匹配（大小写不敏感）
		if isStrict {
			if !strings.EqualFold(fmt.Sprint(rowValue), fmt.Sprint(value)) {
				return false
			}
			continue
		}

		// 规则 4：其他字符串字段 → 子串包含（大小写不敏感）
		rowStr, rowIsStr := rowValue.(string)
		valStr, valIsStr := value.(string)
		if rowIsStr && valIsStr {
			if !strings.Contains(strings.ToLower(rowStr), strings.ToLower(valStr)) {
				return false
			}
			continue
		}

		// 兜底：精确匹配（大小写不敏感）
		if !strings.EqualFold(fmt.Sprint(rowValue), fmt.Sprint(value)) {
			return false
		}
	}
	return true
}

// ApplyFilter 对数据应用过滤条件（FilterEngine 方法）
func (fe *FilterEngine) ApplyFilter(data []map[string]interface{}, predicates []Predicate) []map[string]interface{} {
	result := make([]map[string]interface{}, 0, len(data))
	for _, record := range data {
		if fe.match(record, predicates) {
			result = append(result, record)
		}
	}
	return result
}

// match 检查单条记录是否匹配所有条件
func (fe *FilterEngine) match(record map[string]interface{}, predicates []Predicate) bool {
	for _, p := range predicates {
		if !fe.matchOne(record, p) {
			return false
		}
	}
	return true
}

// matchOne 检查单个条件
func (fe *FilterEngine) matchOne(record map[string]interface{}, p Predicate) bool {
	fieldValue, exists := record[p.Field]
	if !exists {
		return false
	}

	switch p.Operator {
	case "eq", "=":
		return fe.compare(fieldValue, p.Value) == 0
	case "ne", "!=":
		return fe.compare(fieldValue, p.Value) != 0
	case "gt", ">":
		return fe.compare(fieldValue, p.Value) > 0
	case "gte", ">=":
		return fe.compare(fieldValue, p.Value) >= 0
	case "lt", "<":
		return fe.compare(fieldValue, p.Value) < 0
	case "lte", "<=":
		return fe.compare(fieldValue, p.Value) <= 0
	case "in":
		return fe.inArray(fieldValue, p.Value)
	case "contains":
		return fe.contains(fieldValue, p.Value)
	default:
		return false
	}
}

// compare 比较两个值（支持数字、字符串、时间）
func (fe *FilterEngine) compare(a, b interface{}) int {
	// 尝试数字比较
	if aNum, aOk := toFloat64(a); aOk {
		if bNum, bOk := toFloat64(b); bOk {
			if aNum < bNum {
				return -1
			} else if aNum > bNum {
				return 1
			}
			return 0
		}
	}

	// 字符串比较
	aStr := fmt.Sprintf("%v", a)
	bStr := fmt.Sprintf("%v", b)
	return strings.Compare(aStr, bStr)
}

// inArray 检查值是否在数组中
func (fe *FilterEngine) inArray(value, array interface{}) bool {
	arr, ok := array.([]interface{})
	if !ok {
		return false
	}
	for _, v := range arr {
		if fe.compare(value, v) == 0 {
			return true
		}
	}
	return false
}

// contains 检查字符串是否包含子串
func (fe *FilterEngine) contains(value, substr interface{}) bool {
	str := fmt.Sprintf("%v", value)
	sub := fmt.Sprintf("%v", substr)
	return strings.Contains(str, sub)
}

// ParsePredicates 从 map 解析过滤条件
func ParsePredicates(inputs map[string]interface{}, schema *EntitySchema) ([]Predicate, error) {
	predicates := make([]Predicate, 0)

	for fieldName, value := range inputs {
		// 跳过系统字段
		if strings.HasPrefix(fieldName, "__") {
			continue
		}

		// 检查字段是否存在于 schema
		if schema != nil {
			if _, ok := schema.Properties[fieldName]; !ok {
				continue // 跳过未知字段
			}
		}

		// 根据值类型推断操作符
		op := "eq"
		switch v := value.(type) {
		case []interface{}:
			// 数组值表示范围或枚举
			if len(v) == 2 {
				// 可能是时间范围或数值范围
				op = "range"
			} else {
				op = "in"
			}
		case string:
			// 检查是否包含通配符
			if strings.Contains(v, "*") || strings.Contains(v, "?") {
				op = "like"
			}
		}

		predicates = append(predicates, Predicate{
			Field:    fieldName,
			Operator: op,
			Value:    value,
		})
	}

	return predicates, nil
}

// HashSemiJoin 执行 Hash Semi-Join（用于外键关联过滤）
func HashSemiJoin(left []map[string]interface{}, right []map[string]interface{},
	leftKey, rightKey string) []map[string]interface{} {

	// 构建右表 Hash Set
	rightSet := make(map[interface{}]bool)
	for _, r := range right {
		if v, ok := r[rightKey]; ok {
			rightSet[v] = true
		}
	}

	// 过滤左表
	result := make([]map[string]interface{}, 0)
	for _, l := range left {
		if v, ok := l[leftKey]; ok && rightSet[v] {
			result = append(result, l)
		}
	}
	return result
}

// toFloat64 将值转换为 float64
func toFloat64(v interface{}) (float64, bool) {
	switch val := v.(type) {
	case float64:
		return val, true
	case float32:
		return float64(val), true
	case int:
		return float64(val), true
	case int32:
		return float64(val), true
	case int64:
		return float64(val), true
	case string:
		if f, err := strconv.ParseFloat(val, 64); err == nil {
			return f, true
		}
	}
	return 0, false
}

// FilterByTimeRange 按时间范围过滤（特殊优化）
func FilterByTimeRange(data []map[string]interface{}, field string, start, end time.Time) []map[string]interface{} {
	result := make([]map[string]interface{}, 0)
	for _, record := range data {
		if v, ok := record[field]; ok {
			if t, ok := parseTime(v); ok {
				if (start.IsZero() || !t.Before(start)) && (end.IsZero() || !t.After(end)) {
					result = append(result, record)
				}
			}
		}
	}
	return result
}

// parseTime 解析时间值
func parseTime(v interface{}) (time.Time, bool) {
	switch t := v.(type) {
	case time.Time:
		return t, true
	case string:
		// 尝试多种格式
		formats := []string{
			time.RFC3339,
			"2006-01-02 15:04:05",
			"2006-01-02",
		}
		for _, f := range formats {
			if parsed, err := time.Parse(f, t); err == nil {
				return parsed, true
			}
		}
	}
	return time.Time{}, false
}

// CompileFilter 编译过滤器（用于复杂查询预编译）
func CompileFilter(filterExpr string) (*Predicate, error) {
	// TODO: 实现 DSL 解析器，支持类似 "price > 100 AND status = 'active'"
	return nil, fmt.Errorf("not implemented")
}

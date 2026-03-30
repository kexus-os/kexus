// ==============================================================================
// Schema 模块单元测试
// ==============================================================================

package engine

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// TestInferProperties 测试类型推断
func TestInferProperties(t *testing.T) {
	se := NewSchemaEngine("")

	// 测试数据：混合类型（使用超过15个不同的 name 值避免被坍缩为 enum）
	rows := []map[string]any{
		{"id": 1, "name": "Alice", "age": 25, "active": true, "score": 95.5},
		{"id": 2, "name": "Bob", "age": 30, "active": false, "score": 88.0},
		{"id": 3, "name": "Charlie", "age": nil, "active": true, "score": 92.5},
		{"id": 4, "name": "David", "age": 35, "active": true, "score": 90.0},
		{"id": 5, "name": "Eve", "age": 28, "active": false, "score": 85.5},
		{"id": 6, "name": "Frank", "age": 32, "active": true, "score": 91.0},
		{"id": 7, "name": "Grace", "age": 27, "active": true, "score": 93.5},
		{"id": 8, "name": "Henry", "age": 33, "active": false, "score": 87.0},
		{"id": 9, "name": "Ivy", "age": 29, "active": true, "score": 89.5},
		{"id": 10, "name": "Jack", "age": 31, "active": false, "score": 86.0},
		{"id": 11, "name": "Kate", "age": 26, "active": true, "score": 94.0},
		{"id": 12, "name": "Leo", "age": 34, "active": true, "score": 88.5},
		{"id": 13, "name": "Mary", "age": 30, "active": false, "score": 92.0},
		{"id": 14, "name": "Nick", "age": 28, "active": true, "score": 90.5},
		{"id": 15, "name": "Olivia", "age": 32, "active": false, "score": 87.5},
		{"id": 16, "name": "Peter", "age": 29, "active": true, "score": 91.5},
	}

	props := se.InferProperties(rows, 100)

	// 验证 id 为 integer
	if props["id"].Type != "integer" {
		t.Errorf("expected id type 'integer', got '%s'", props["id"].Type)
	}
	if !props["id"].Required {
		t.Error("expected id to be required")
	}

	// 验证 name 为 string（超过15个唯一值，不会被坍缩为 enum）
	if props["name"].Type != "string" {
		t.Errorf("expected name type 'string', got '%s'", props["name"].Type)
	}

	// 验证 age 为 integer 且 nullable（因为有 nil 值）
	if props["age"].Type != "integer" {
		t.Errorf("expected age type 'integer', got '%s'", props["age"].Type)
	}
	if !props["age"].Nullable {
		t.Error("expected age to be nullable")
	}

	// 验证 active 为 boolean（bool 判断必须在 int 之前）
	if props["active"].Type != "boolean" {
		t.Errorf("expected active type 'boolean', got '%s'", props["active"].Type)
	}

	// 验证 score 为 float
	if props["score"].Type != "float" {
		t.Errorf("expected score type 'float', got '%s'", props["score"].Type)
	}
}

// TestInferPropertiesEnum 测试 enum 坍缩
func TestInferPropertiesEnum(t *testing.T) {
	se := NewSchemaEngine("")

	// 测试数据：status 字段只有 3 个唯一值（应该坍缩为 enum）
	rows := []map[string]any{
		{"id": 1, "status": "active"},
		{"id": 2, "status": "inactive"},
		{"id": 3, "status": "pending"},
		{"id": 4, "status": "active"},
		{"id": 5, "status": "inactive"},
	}

	props := se.InferProperties(rows, 100)

	// status 应该坍缩为 enum
	if props["status"].Type != "enum" {
		t.Errorf("expected status type 'enum', got '%s'", props["status"].Type)
	}
	if len(props["status"].EnumValues) != 3 {
		t.Errorf("expected 3 enum values, got %d", len(props["status"].EnumValues))
	}
}

// TestInferPropertiesNoEnumForTime 测试时间字段不坍缩为 enum
func TestInferPropertiesNoEnumForTime(t *testing.T) {
	se := NewSchemaEngine("")

	// 测试数据：created_at 是时间字段，应该被识别为 datetime
	rows := []map[string]any{
		{"id": 1, "created_at": "2024-01-01T10:00:00Z"},
		{"id": 2, "created_at": "2024-01-02T11:00:00Z"},
		{"id": 3, "created_at": "2024-01-03T12:00:00Z"},
	}

	props := se.InferProperties(rows, 100)

	// created_at 应该被识别为 datetime（不是 string，也不是 enum）
	if props["created_at"].Type != "datetime" {
		t.Errorf("expected created_at type 'datetime' (not string/enum), got '%s'", props["created_at"].Type)
	}
}

// TestEvolve 测试 Schema 进化
func TestEvolve(t *testing.T) {
	// 创建临时目录
	tmpDir := t.TempDir()
	se := NewSchemaEngine(tmpDir)

	// 第一次进化：创建 v1
	props1 := map[string]SchemaProperty{
		"id":   {Type: "integer", Required: true},
		"name": {Type: "string", Required: true},
	}

	ver1, err := se.Evolve("test_entity", props1, "id")
	if err != nil {
		t.Fatalf("Evolve failed: %v", err)
	}
	if ver1 != "v1" {
		t.Errorf("expected version v1, got %s", ver1)
	}

	// 验证文件存在
	schemaPath := filepath.Join(tmpDir, "test_entity", "v1.json")
	if _, err := os.Stat(schemaPath); os.IsNotExist(err) {
		t.Error("schema file v1.json should exist")
	}

	// 第二次进化：相同 properties，应该返回 v1（不创建新版本）
	ver2, err := se.Evolve("test_entity", props1, "id")
	if err != nil {
		t.Fatalf("Evolve failed: %v", err)
	}
	if ver2 != "v1" {
		t.Errorf("expected version v1 (same properties), got %s", ver2)
	}

	// 验证没有创建 v2
	schemaPathV2 := filepath.Join(tmpDir, "test_entity", "v2.json")
	if _, err := os.Stat(schemaPathV2); !os.IsNotExist(err) {
		t.Error("schema file v2.json should not exist (properties not changed)")
	}

	// 第三次进化：不同 properties，应该创建 v2
	props2 := map[string]SchemaProperty{
		"id":    {Type: "integer", Required: true},
		"name":  {Type: "string", Required: true},
		"email": {Type: "string", Required: false}, // 新增字段
	}

	ver3, err := se.Evolve("test_entity", props2, "id")
	if err != nil {
		t.Fatalf("Evolve failed: %v", err)
	}
	if ver3 != "v2" {
		t.Errorf("expected version v2, got %s", ver3)
	}

	// 验证 v2 文件存在
	if _, err := os.Stat(schemaPathV2); os.IsNotExist(err) {
		t.Error("schema file v2.json should exist")
	}
}

// TestEvolvePropertiesEqual 测试 properties 相等判断
func TestEvolvePropertiesEqual(t *testing.T) {
	// 相同的 properties
	a := map[string]SchemaProperty{
		"id":   {Type: "integer", Required: true, Nullable: false},
		"name": {Type: "string", Required: true, Nullable: false},
	}
	b := map[string]SchemaProperty{
		"id":   {Type: "integer", Required: true, Nullable: false},
		"name": {Type: "string", Required: true, Nullable: false},
	}

	if !propertiesEqual(a, b) {
		t.Error("properties should be equal")
	}

	// 不同的 type
	c := map[string]SchemaProperty{
		"id":   {Type: "string", Required: true, Nullable: false}, // 不同
		"name": {Type: "string", Required: true, Nullable: false},
	}
	if propertiesEqual(a, c) {
		t.Error("properties should not be equal (different type)")
	}

	// 不同的 enum_values
	d := map[string]SchemaProperty{
		"status": {Type: "enum", EnumValues: []string{"a", "b"}},
	}
	e := map[string]SchemaProperty{
		"status": {Type: "enum", EnumValues: []string{"a", "c"}}, // 不同
	}
	if propertiesEqual(d, e) {
		t.Error("properties should not be equal (different enum values)")
	}
}

// TestInferForeignKeys 测试外键推断
func TestInferForeignKeys(t *testing.T) {
	se := NewSchemaEngine("")

	schema := &EntitySchema{
		EntityName: "order_items",
		Properties: map[string]SchemaProperty{
			"id":         {Type: "integer", Required: true},
			"order_id":   {Type: "integer", Required: true},
			"product_id": {Type: "integer", Required: true},
			"quantity":   {Type: "integer", Required: true},
		},
	}

	// 上游表名列表
	upstream := []string{"orders", "products"}

	fks := se.InferForeignKeys(schema, upstream)

	// 验证 product_id -> products
	if target, ok := fks["product_id"]; !ok || target != "products" {
		t.Errorf("expected product_id -> products, got %s", target)
	}

	// 验证 order_id -> orders
	if target, ok := fks["order_id"]; !ok || target != "orders" {
		t.Errorf("expected order_id -> orders, got %s", target)
	}

	// quantity 不是外键
	if _, ok := fks["quantity"]; ok {
		t.Error("quantity should not be a foreign key")
	}
}

// TestLoadLatestSchema 测试加载最新 Schema
func TestLoadLatestSchema(t *testing.T) {
	// 创建临时目录
	tmpDir := t.TempDir()
	se := NewSchemaEngine(tmpDir)

	// 创建测试实体目录和 v1 schema
	entityDir := filepath.Join(tmpDir, "test_entity")
	os.MkdirAll(entityDir, 0755)

	schema1 := EntitySchema{
		EntityName: "test_entity",
		Version:    "v1",
		PrimaryKey: "id",
		Timestamp:  "2024-01-01T00:00:00Z",
		Properties: map[string]SchemaProperty{
			"id":   {Type: "integer", Required: true},
			"name": {Type: "string", Required: true},
		},
	}
	data1, _ := json.MarshalIndent(schema1, "", "  ")
	os.WriteFile(filepath.Join(entityDir, "v1.json"), data1, 0644)

	// 创建 v2 schema
	schema2 := EntitySchema{
		EntityName: "test_entity",
		Version:    "v2",
		PrimaryKey: "id",
		Timestamp:  "2024-01-02T00:00:00Z",
		Properties: map[string]SchemaProperty{
			"id":    {Type: "integer", Required: true},
			"name":  {Type: "string", Required: true},
			"email": {Type: "string", Required: false},
		},
	}
	data2, _ := json.MarshalIndent(schema2, "", "  ")
	os.WriteFile(filepath.Join(entityDir, "v2.json"), data2, 0644)

	// 加载最新版本
	loaded, err := se.LoadLatestSchema("test_entity")
	if err != nil {
		t.Fatalf("LoadLatestSchema failed: %v", err)
	}
	if loaded == nil {
		t.Fatal("LoadLatestSchema returned nil")
	}

	// 验证是 v2
	if loaded.Version != "v2" {
		t.Errorf("expected version v2, got %s", loaded.Version)
	}

	// 验证 properties 包含 email
	if _, ok := loaded.Properties["email"]; !ok {
		t.Error("expected properties to contain 'email'")
	}
}

// TestSchemaCompatibility 测试与 Python 版 schema 格式兼容
func TestSchemaCompatibility(t *testing.T) {
	// 创建临时目录
	tmpDir := t.TempDir()
	se := NewSchemaEngine(tmpDir)

	// 创建 Python 风格的 schema 文件
	pythonStyleSchema := `{
  "entity_name": "orders",
  "version": "v1",
  "primary_key": "id",
  "timestamp": "2024-01-01T00:00:00Z",
  "properties": {
    "id": {"type": "integer", "required": true},
    "order_no": {"type": "string", "required": true},
    "status": {"type": "enum", "required": true, "enum_values": ["pending", "completed", "cancelled"]},
    "total_amount": {"type": "float", "required": false, "nullable": true}
  }
}`

	entityDir := filepath.Join(tmpDir, "orders")
	os.MkdirAll(entityDir, 0755)
	schemaPath := filepath.Join(entityDir, "v1.json")
	os.WriteFile(schemaPath, []byte(pythonStyleSchema), 0644)

	// 加载并验证
	loaded, err := se.LoadLatestSchema("orders")
	if err != nil {
		t.Fatalf("LoadLatestSchema failed: %v", err)
	}

	// 验证字段
	if loaded.EntityName != "orders" {
		t.Errorf("expected entity_name 'orders', got '%s'", loaded.EntityName)
	}
	if loaded.Properties["status"].Type != "enum" {
		t.Errorf("expected status type 'enum', got '%s'", loaded.Properties["status"].Type)
	}
	if len(loaded.Properties["status"].EnumValues) != 3 {
		t.Errorf("expected 3 enum values, got %d", len(loaded.Properties["status"].EnumValues))
	}
	if !loaded.Properties["total_amount"].Nullable {
		t.Error("expected total_amount to be nullable")
	}

	// 验证 Go 写入的格式 Python 能读取
	props := map[string]SchemaProperty{
		"id":       {Type: "integer", Required: true},
		"name":     {Type: "string", Required: true},
		"category": {Type: "enum", Required: false, EnumValues: []string{"A", "B", "C"}},
	}
	ver, err := se.Evolve("test_compat", props, "id")
	if err != nil {
		t.Fatalf("Evolve failed: %v", err)
	}

	// 读取写入的文件内容，验证是合法的 JSON
	compatPath := filepath.Join(tmpDir, "test_compat", ver+".json")
	content, err := os.ReadFile(compatPath)
	if err != nil {
		t.Fatalf("Failed to read written schema: %v", err)
	}

	var verify map[string]any
	if err := json.Unmarshal(content, &verify); err != nil {
		t.Errorf("Written schema is not valid JSON: %v", err)
	}

	// 验证必要字段存在
	requiredFields := []string{"entity_name", "version", "primary_key", "timestamp", "properties"}
	for _, field := range requiredFields {
		if _, ok := verify[field]; !ok {
			t.Errorf("Required field '%s' not found in written schema", field)
		}
	}
}

// TestInferTypeWithIntFloat 测试整数和浮点数推断
func TestInferTypeWithIntFloat(t *testing.T) {
	se := NewSchemaEngine("")

	// 测试数据：mixed 整数和浮点数
	rows := []map[string]any{
		{"id": 1, "price": 100.0},    // price 是整数形式的浮点
		{"id": 2, "price": 99.99},    // price 是小数
		{"id": 3, "price": 50.0},     // price 是整数形式的浮点
	}

	props := se.InferProperties(rows, 100)

	// id 应该是 integer
	if props["id"].Type != "integer" {
		t.Errorf("expected id type 'integer', got '%s'", props["id"].Type)
	}

	// price 应该是 float（因为有些值是小数）
	if props["price"].Type != "float" {
		t.Errorf("expected price type 'float', got '%s'", props["price"].Type)
	}
}

// TestGetLatestVersion 测试获取最新版本号
func TestGetLatestVersion(t *testing.T) {
	// 创建临时目录
	tmpDir := t.TempDir()
	se := NewSchemaEngine(tmpDir)

	// 测试不存在的实体
	ver, err := se.GetLatestVersion("nonexistent")
	if err != nil {
		t.Fatalf("GetLatestVersion failed: %v", err)
	}
	if ver != 0 {
		t.Errorf("expected version 0 for nonexistent entity, got %d", ver)
	}

	// 创建测试实体目录和 schema 文件
	entityDir := filepath.Join(tmpDir, "test_entity")
	os.MkdirAll(entityDir, 0755)

	// 创建 v1, v3, v5（跳过 v2, v4）
	os.WriteFile(filepath.Join(entityDir, "v1.json"), []byte("{}"), 0644)
	os.WriteFile(filepath.Join(entityDir, "v3.json"), []byte("{}"), 0644)
	os.WriteFile(filepath.Join(entityDir, "v5.json"), []byte("{}"), 0644)

	// 测试获取最新版本
	ver, err = se.GetLatestVersion("test_entity")
	if err != nil {
		t.Fatalf("GetLatestVersion failed: %v", err)
	}
	if ver != 5 {
		t.Errorf("expected version 5, got %d", ver)
	}
}

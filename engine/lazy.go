// ==============================================================================
// 🔄 Lazy 模块
// 负责：流式 JSONL/JSON 迭代器、内存友好的大数据处理
// ==============================================================================

package engine

import (
	"bufio"
	"encoding/json"
	"io"
	"os"
)

// RecordIterator 数据记录迭代器接口
type RecordIterator interface {
	Next() (map[string]interface{}, bool)
	Reset() error
	Close() error
}

// JSONLIterator JSONL 文件迭代器（流式读取）
type JSONLIterator struct {
	file    *os.File
	scanner *bufio.Scanner
	reader  io.Reader
}

// NewJSONLIterator 创建 JSONL 迭代器
func NewJSONLIterator(filepath string) (*JSONLIterator, error) {
	file, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}

	return &JSONLIterator{
		file:    file,
		scanner: bufio.NewScanner(file),
		reader:  file,
	}, nil
}

// Next 读取下一条记录
func (it *JSONLIterator) Next() (map[string]interface{}, bool) {
	if !it.scanner.Scan() {
		return nil, false
	}

	line := it.scanner.Bytes()
	if len(line) == 0 {
		return nil, true // 空行，继续下一条
	}

	var record map[string]interface{}
	if err := json.Unmarshal(line, &record); err != nil {
		return nil, false // 解析失败
	}

	return record, true
}

// Reset 重置迭代器到开头
func (it *JSONLIterator) Reset() error {
	if _, err := it.file.Seek(0, 0); err != nil {
		return err
	}
	it.scanner = bufio.NewScanner(it.file)
	return nil
}

// Close 关闭迭代器
func (it *JSONLIterator) Close() error {
	return it.file.Close()
}

// JSONIterator JSON 数组文件迭代器
type JSONIterator struct {
	file     *os.File
	data     []map[string]interface{}
	index    int
	filepath string
}

// NewJSONIterator 创建 JSON 迭代器
func NewJSONIterator(filepath string) (*JSONIterator, error) {
	file, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var data []map[string]interface{}
	if err := json.NewDecoder(file).Decode(&data); err != nil {
		return nil, err
	}

	return &JSONIterator{
		data:     data,
		index:    0,
		filepath: filepath,
	}, nil
}

// Next 读取下一条记录
func (it *JSONIterator) Next() (map[string]interface{}, bool) {
	if it.index >= len(it.data) {
		return nil, false
	}
	record := it.data[it.index]
	it.index++
	return record, true
}

// Reset 重置迭代器
func (it *JSONIterator) Reset() error {
	it.index = 0
	return nil
}

// Close 关闭迭代器（JSON 迭代器不需要关闭文件）
func (it *JSONIterator) Close() error {
	return nil
}

// Len 返回总记录数
func (it *JSONIterator) Len() int {
	return len(it.data)
}

// LazyFilter 惰性过滤迭代器（不加载全部数据到内存）
type LazyFilter struct {
	source    RecordIterator
	predicate func(map[string]interface{}) bool
}

// NewLazyFilter 创建惰性过滤迭代器
func NewLazyFilter(source RecordIterator, predicate func(map[string]interface{}) bool) *LazyFilter {
	return &LazyFilter{
		source:    source,
		predicate: predicate,
	}
}

// Next 读取下一条符合条件的记录
func (lf *LazyFilter) Next() (map[string]interface{}, bool) {
	for {
		record, ok := lf.source.Next()
		if !ok {
			return nil, false
		}
		if record == nil {
			continue
		}
		if lf.predicate(record) {
			return record, true
		}
	}
}

// Reset 重置
func (lf *LazyFilter) Reset() error {
	return lf.source.Reset()
}

// Close 关闭
func (lf *LazyFilter) Close() error {
	return lf.source.Close()
}

// LazyProject 惰性投影迭代器（只选择需要的字段）
type LazyProject struct {
	source  RecordIterator
	fields  []string
}

// NewLazyProject 创建惰性投影迭代器
func NewLazyProject(source RecordIterator, fields []string) *LazyProject {
	return &LazyProject{
		source: source,
		fields: fields,
	}
}

// Next 读取下一条记录并投影
func (lp *LazyProject) Next() (map[string]interface{}, bool) {
	record, ok := lp.source.Next()
	if !ok || record == nil {
		return nil, false
	}

	projected := make(map[string]interface{})
	for _, field := range lp.fields {
		if val, ok := record[field]; ok {
			projected[field] = val
		}
	}
	return projected, true
}

// Reset 重置
func (lp *LazyProject) Reset() error {
	return lp.source.Reset()
}

// Close 关闭
func (lp *LazyProject) Close() error {
	return lp.source.Close()
}

// ToSlice 将迭代器转换为切片（注意：可能占用大量内存）
func ToSlice(it RecordIterator) ([]map[string]interface{}, error) {
	var result []map[string]interface{}
	for {
		record, ok := it.Next()
		if !ok {
			break
		}
		if record != nil {
			result = append(result, record)
		}
	}
	return result, nil
}

// Count 统计迭代器中的记录数
func Count(it RecordIterator) (int, error) {
	count := 0
	for {
		_, ok := it.Next()
		if !ok {
			break
		}
		count++
	}
	return count, nil
}

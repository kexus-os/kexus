// ==============================================================================
// 🚀 Kexus OS v3.0 — Go Data Engine
// 数据引擎统一入口，提供 Schema、Filter、Exchange、Backup 等核心能力
// ==============================================================================

package engine

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
)

// DataEngine 是数据引擎的主结构
type DataEngine struct {
	ProjectDir string
	SchemaDir  string
	DataDir    string
	ExchangeDB string
	BackupDir  string
}

// NewDataEngine 创建数据引擎实例
func NewDataEngine(projectDir string) *DataEngine {
	return &DataEngine{
		ProjectDir: projectDir,
		SchemaDir:  filepath.Join(projectDir, "meta_schema"),
		DataDir:    filepath.Join(projectDir, "data"),
		ExchangeDB: filepath.Join(projectDir, "data", ".exchange", "kexus_bus.db"),
		BackupDir:  filepath.Join(projectDir, "data", ".backup"),
	}
}

// Init 初始化数据引擎（目录、数据库等）
func (e *DataEngine) Init() error {
	// 创建必要目录
	dirs := []string{
		e.SchemaDir,
		e.DataDir,
		filepath.Join(e.DataDir, ".exchange"),
		e.BackupDir,
	}
	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("创建目录失败 %s: %w", dir, err)
		}
	}

	// 初始化 Exchange 数据库
	if err := e.initExchange(); err != nil {
		return fmt.Errorf("初始化 Exchange 失败: %w", err)
	}

	log.Println("✅ [DataEngine] 初始化完成")
	return nil
}

// initExchange 初始化 SQLite Exchange 数据库
func (e *DataEngine) initExchange() error {
	// TODO: 实现 Exchange 初始化
	// 创建表：exchange_pointers, exchange_data 等
	return nil
}

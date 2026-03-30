// =============================================================================
// 💾 Exchange Manager — DataBus 数据交换层管理
// 负责：Exchange 数据写入、读取、自动清理
// =============================================================================

package engine

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

var (
	exchangeDBPath string
	exchangeOnce   sync.Once
)

// InitExchangeManager 初始化 Exchange 管理器
func InitExchangeManager(projectDir string) {
	exchangeDir := filepath.Join(projectDir, "data", ".exchange")
	os.MkdirAll(exchangeDir, 0755)
	exchangeDBPath = filepath.Join(exchangeDir, "kexus_bus.db")

	// 确保表存在
	db, err := sql.Open("sqlite", exchangeDBPath)
	if err != nil {
		fmt.Printf("⚠️ [exchange] 初始化失败: %v\n", err)
		return
	}
	defer db.Close()

	schema := `
	CREATE TABLE IF NOT EXISTS exchange_store (
		pointer_id TEXT PRIMARY KEY,
		timestamp REAL NOT NULL,
		payload TEXT NOT NULL
	);
	CREATE INDEX IF NOT EXISTS idx_exchange_time ON exchange_store(timestamp);
	`
	if _, err := db.Exec(schema); err != nil {
		fmt.Printf("⚠️ [exchange] 建表失败: %v\n", err)
	}
}

// WriteExchange 写入数据到 Exchange，返回指针
func WriteExchange(opID string, data []map[string]interface{}) map[string]interface{} {
	pointerID := fmt.Sprintf("ptr_%s_%d_%d", opID, time.Now().UnixMilli(), len(data))

	db, err := sql.Open("sqlite", exchangeDBPath)
	if err != nil {
		// 降级到内存存储
		return map[string]interface{}{
			"__kexus_exchange__": pointerID,
			"_fallback":          "memory",
		}
	}
	defer db.Close()

	payload, _ := json.Marshal(data)
	_, err = db.Exec(
		"INSERT INTO exchange_store (pointer_id, timestamp, payload) VALUES (?, ?, ?)",
		pointerID, float64(time.Now().UnixMilli())/1000.0, string(payload),
	)
	if err != nil {
		return map[string]interface{}{
			"__kexus_exchange__": pointerID,
			"_error":             err.Error(),
		}
	}

	return map[string]interface{}{
		"__kexus_exchange__": pointerID,
	}
}

// ReadExchange 从 Exchange 读取数据
func ReadExchange(pointerID string) ([]map[string]interface{}, error) {
	db, err := sql.Open("sqlite", exchangeDBPath)
	if err != nil {
		return nil, fmt.Errorf("打开 Exchange DB 失败: %w", err)
	}
	defer db.Close()

	var payload string
	err = db.QueryRow(
		"SELECT payload FROM exchange_store WHERE pointer_id = ?",
		pointerID,
	).Scan(&payload)
	if err != nil {
		return nil, fmt.Errorf("读取失败: %w", err)
	}

	var data []map[string]interface{}
	if err := json.Unmarshal([]byte(payload), &data); err != nil {
		return nil, fmt.Errorf("解析失败: %w", err)
	}
	return data, nil
}

// CleanupExchange 清理过期的 Exchange 记录（默认保留1小时）
func CleanupExchange(maxAge time.Duration) error {
	if exchangeDBPath == "" {
		return fmt.Errorf("Exchange 未初始化")
	}

	db, err := sql.Open("sqlite", exchangeDBPath)
	if err != nil {
		return err
	}
	defer db.Close()

	cutoff := float64(time.Now().Add(-maxAge).UnixMilli()) / 1000.0
	result, err := db.Exec("DELETE FROM exchange_store WHERE timestamp < ?", cutoff)
	if err != nil {
		return err
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected > 0 {
		fmt.Printf("🧹 [exchange] 已清理 %d 条过期记录\n", rowsAffected)
	}
	return nil
}

// StartExchangeCleanup 启动定时清理任务
func StartExchangeCleanup(intervalMinutes int) {
	if intervalMinutes <= 0 {
		return
	}
	go func() {
		ticker := time.NewTicker(time.Duration(intervalMinutes) * time.Minute)
		defer ticker.Stop()
		for range ticker.C {
			if err := CleanupExchange(time.Hour); err != nil {
				fmt.Printf("⚠️ [exchange] 清理失败: %v\n", err)
			}
		}
	}()
	fmt.Printf("🧹 [exchange] 定时清理已启动: 每 %d 分钟\n", intervalMinutes)
}

// GetExchangeStats 获取 Exchange 统计信息
func GetExchangeStats() (recordCount int, dbSize int64, err error) {
	if exchangeDBPath == "" {
		return 0, 0, fmt.Errorf("Exchange 未初始化")
	}

	db, err := sql.Open("sqlite", exchangeDBPath)
	if err != nil {
		return 0, 0, err
	}
	defer db.Close()

	err = db.QueryRow("SELECT COUNT(*) FROM exchange_store").Scan(&recordCount)
	if err != nil {
		return 0, 0, err
	}

	info, err := os.Stat(exchangeDBPath)
	if err != nil {
		return recordCount, 0, nil
	}
	return recordCount, info.Size(), nil
}

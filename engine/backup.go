// =============================================================================
// 🛡️ Kexus OS v3.0 — Backup Manager
// SQLite 数据库自动快照与清理
// =============================================================================

package engine

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

// BackupManager SQLite 备份管理器
type BackupManager struct {
	primaryDBPath string // 主库路径
	backupDir     string // 备份目录
	retention     int    // 保留快照数量
	logFn         func(string) // 日志回调
}

// InitBackupManager 初始化备份管理器
func InitBackupManager(primaryDBPath, backupDir string, retention int, logFn func(string)) *BackupManager {
	if logFn == nil {
		logFn = func(s string) { fmt.Println(s) }
	}
	os.MkdirAll(backupDir, 0755)
	return &BackupManager{
		primaryDBPath: primaryDBPath,
		backupDir:     backupDir,
		retention:     retention,
		logFn:         logFn,
	}
}

// Snapshot 执行一次完整快照
// 使用 SQLite 的 VACUUM INTO 创建一致性快照，不阻塞读写
func (bm *BackupManager) Snapshot() error {
	// 检查主库是否存在
	if _, err := os.Stat(bm.primaryDBPath); os.IsNotExist(err) {
		return fmt.Errorf("主库不存在: %s", bm.primaryDBPath)
	}

	timestamp := time.Now().Format("20060102_150405")
	dbName := strings.TrimSuffix(filepath.Base(bm.primaryDBPath), ".db")
	destPath := filepath.Join(bm.backupDir, fmt.Sprintf("%s_snapshot_%s.db", dbName, timestamp))

	db, err := sql.Open("sqlite", bm.primaryDBPath)
	if err != nil {
		return fmt.Errorf("打开主库失败: %w", err)
	}
	defer db.Close()

	// VACUUM INTO 创建一致性快照
	_, err = db.Exec(fmt.Sprintf(`VACUUM INTO '%s'`, destPath))
	if err != nil {
		return fmt.Errorf("快照失败: %w", err)
	}

	bm.logFn(fmt.Sprintf("🛡️ [backup] 快照已创建: %s", filepath.Base(destPath)))

	// 清理旧快照
	bm.pruneOldSnapshots()
	return nil
}

// pruneOldSnapshots 清理超出保留数量的旧快照
func (bm *BackupManager) pruneOldSnapshots() {
	entries, err := os.ReadDir(bm.backupDir)
	if err != nil {
		bm.logFn(fmt.Sprintf("⚠️ [backup] 读取备份目录失败: %v", err))
		return
	}

	dbName := strings.TrimSuffix(filepath.Base(bm.primaryDBPath), ".db")
	prefix := dbName + "_snapshot_"

	var snapshots []string
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), prefix) && strings.HasSuffix(e.Name(), ".db") {
			snapshots = append(snapshots, e.Name())
		}
	}

	// 时间戳命名，字典序 = 时间序
	sort.Strings(snapshots)

	if len(snapshots) > bm.retention {
		toDelete := snapshots[:len(snapshots)-bm.retention]
		for _, name := range toDelete {
			path := filepath.Join(bm.backupDir, name)
			if err := os.Remove(path); err != nil {
				bm.logFn(fmt.Sprintf("⚠️ [backup] 删除旧快照失败 %s: %v", name, err))
			} else {
				bm.logFn(fmt.Sprintf("🗑️ [backup] 已清理旧快照: %s", name))
			}
		}
	}
}

// StartPeriodicBackup 启动定时快照 goroutine
// intervalMinutes 为 0 时不启动
func (bm *BackupManager) StartPeriodicBackup(intervalMinutes int) {
	if intervalMinutes <= 0 {
		return
	}

	// 立即执行一次快照
	if err := bm.Snapshot(); err != nil {
		bm.logFn(fmt.Sprintf("⚠️ [backup] 初始快照失败: %v", err))
	}

	go func() {
		ticker := time.NewTicker(time.Duration(intervalMinutes) * time.Minute)
		defer ticker.Stop()
		for range ticker.C {
			if err := bm.Snapshot(); err != nil {
				bm.logFn(fmt.Sprintf("⚠️ [backup] 定时快照失败: %v", err))
			}
		}
	}()

	bm.logFn(fmt.Sprintf("🛡️ [backup] 定时备份已启动: 间隔 %d 分钟", intervalMinutes))
}

// ListSnapshots 列出所有可用快照（按时间倒序）
func (bm *BackupManager) ListSnapshots() ([]string, error) {
	entries, err := os.ReadDir(bm.backupDir)
	if err != nil {
		return nil, err
	}

	dbName := strings.TrimSuffix(filepath.Base(bm.primaryDBPath), ".db")
	prefix := dbName + "_snapshot_"

	var snapshots []string
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), prefix) && strings.HasSuffix(e.Name(), ".db") {
			snapshots = append(snapshots, e.Name())
		}
	}

	// 倒序排列（最新的在前）
	sort.Sort(sort.Reverse(sort.StringSlice(snapshots)))
	return snapshots, nil
}

// RestoreSnapshot 从指定快照恢复
// snapshotName 为 "" 时表示最新的快照
func (bm *BackupManager) RestoreSnapshot(snapshotName string) error {
	snapshots, err := bm.ListSnapshots()
	if err != nil {
		return err
	}
	if len(snapshots) == 0 {
		return fmt.Errorf("没有可用的快照")
	}

	targetSnapshot := snapshotName
	if targetSnapshot == "" {
		targetSnapshot = snapshots[0] // 最新的
	}

	srcPath := filepath.Join(bm.backupDir, targetSnapshot)
	if _, err := os.Stat(srcPath); os.IsNotExist(err) {
		return fmt.Errorf("快照不存在: %s", targetSnapshot)
	}

	// 备份当前主库（以防万一）
	backupCurrent := bm.primaryDBPath + ".backup_" + time.Now().Format("20060102_150405")
	if _, err := os.Stat(bm.primaryDBPath); err == nil {
		if err := copyFile(bm.primaryDBPath, backupCurrent); err != nil {
			return fmt.Errorf("备份当前库失败: %w", err)
		}
	}

	// 关闭所有连接后替换文件
	if err := copyFile(srcPath, bm.primaryDBPath); err != nil {
		return fmt.Errorf("恢复快照失败: %w", err)
	}

	bm.logFn(fmt.Sprintf("🛡️ [backup] 已从快照恢复: %s", targetSnapshot))
	return nil
}

// copyFile 复制文件
func copyFile(src, dst string) error {
	data, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	return os.WriteFile(dst, data, 0644)
}

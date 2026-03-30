package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"html"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rs/cors"
	"kexus/engine"
)

// ==============================================================================
// 📦 Vendor 本地化
// ==============================================================================

var vendorFiles = map[string]string{
	"vue.js":                  "https://unpkg.com/vue@3.5.13/dist/vue.global.prod.js",
	"tailwind.js":             "https://cdn.tailwindcss.com",
	"codemirror.min.css":      "https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.16/codemirror.min.css",
	"material-darker.min.css": "https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.16/theme/material-darker.min.css",
	"codemirror.min.js":       "https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.16/codemirror.min.js",
	"cm-python.min.js":        "https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.16/mode/python/python.min.js",
	"cm-xml.min.js":           "https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.16/mode/xml/xml.min.js",
	"cm-javascript.min.js":    "https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.16/mode/javascript/javascript.min.js",
	"cm-htmlmixed.min.js":     "https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.16/mode/htmlmixed/htmlmixed.min.js",
	"cm-matchbrackets.min.js": "https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.16/addon/edit/matchbrackets.min.js",
	"cm-closebrackets.min.js": "https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.16/addon/edit/closebrackets.min.js",
	"cm-searchcursor.min.js":  "https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.16/addon/search/searchcursor.min.js",
	"cm-comment.min.js":       "https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.16/addon/comment/comment.min.js",
	"cm-sublime.min.js":       "https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.16/keymap/sublime.min.js",
}

func ensureVendorFiles() {
	vendorDir := filepath.Join(ProjectDir, "vendor")
	if err := os.MkdirAll(vendorDir, 0755); err != nil {
		log.Printf("⚠️  [vendor] 无法创建 vendor/ 目录: %v", err)
		return
	}
	client := &http.Client{Timeout: 30 * time.Second}
	for name, url := range vendorFiles {
		dest := filepath.Join(vendorDir, name)
		if _, err := os.Stat(dest); err == nil {
			continue
		}
		fmt.Printf("📦 [vendor] 下载 %s ...\n", name)
		resp, err := client.Get(url)
		if err != nil {
			log.Printf("⚠️  [vendor] 下载 %s 失败: %v", name, err)
			continue
		}
		data, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil || resp.StatusCode != 200 {
			log.Printf("⚠️  [vendor] %s HTTP %d", name, resp.StatusCode)
			continue
		}
		if err := os.WriteFile(dest, data, 0644); err != nil {
			log.Printf("⚠️  [vendor] 写入 %s 失败: %v", name, err)
			continue
		}
		fmt.Printf("✅ [vendor] %s (%d KB)\n", name, len(data)/1024)
	}
}

// ==============================================================================
// 🌌 环境张量与物理边界配置
// ==============================================================================

var (
	ProjectDir string
	PythonExec string
	ConfigFile = `kexus_state.json`
	CacheFile  = `kexus_flow_cache.json`  // v3.1: Flow 调试数据缓存文件

	// v3.0: 扩展配置变量
	ServerPort      string
	UseGoEngine     bool
	BackupDir       string
	BackupInterval  int
	BackupRetention int
	FlowTimeout     int
	OpTimeout       int

	// v3.1: 域管理配置
	ZoneDir        string // zone 根目录，例如 ./zone
	CurrentZone    string // 当前激活的域，例如 _yunyao
	BaseProjectDir string // 原始项目目录
)

type Op struct {
	ID             string          `json:"id"`
	Name           string          `json:"name"`
	Cmd            string          `json:"cmd"`
	Tags           []string        `json:"tags"`
	Locked         bool            `json:"locked"`
	Private        bool            `json:"private"`
	Meta           json.RawMessage `json:"meta,omitempty"`
	Intent         string          `json:"intent,omitempty"`
	SchemaEntity   string          `json:"schema_entity,omitempty"`
	SchemaVersion  string          `json:"schema_version,omitempty"`
	SchemaHash     string          `json:"schema_hash,omitempty"`
	ProducesTopics []string        `json:"produces_topics,omitempty"`
	ConsumesTopics []string        `json:"consumes_topics,omitempty"`
	LLMBinding     string          `json:"llm_binding,omitempty"` // v3.0: 算子绑定的 LLM 配置名
}

type Node struct {
	OpID   string                 `json:"op_id"`
	Active bool                   `json:"active"`
	Inputs map[string]interface{} `json:"inputs"`
}

// ScheduleTrigger 多时间点触发配置
type ScheduleTrigger struct {
	Time string `json:"time"` // HH:MM 格式
	Desc string `json:"desc"` // 描述
	Days []int  `json:"days"` // 生效的星期 [0-6]
}

type Flow struct {
	ID                      string                     `json:"id"`
	Name                    string                     `json:"name"`
	Nodes                   []Node                     `json:"nodes"`
	Status                  string                     `json:"status"`
	LastError               string                     `json:"last_error"`
	ScheduleTime            string                     `json:"schedule_time"`
	ScheduleDays            []int                      `json:"schedule_days"`
	ScheduleEnabled         bool                       `json:"schedule_enabled"`
	ScheduleMode            string                     `json:"schedule_mode"` // "daily", "interval", "multi_trigger"
	ScheduleIntervalHours   int                        `json:"schedule_interval_hours"`
	ScheduleIntervalMinutes int                        `json:"schedule_interval_minutes"`
	ScheduleIntervalSeconds int                        `json:"schedule_interval_seconds"`
	ScheduleTriggers        []ScheduleTrigger          `json:"schedule_triggers,omitempty"` // 🚀 多时间点触发配置
	Locked                  bool                       `json:"locked"`
	Private                 bool                       `json:"private"`
	Meta                    json.RawMessage            `json:"meta,omitempty"`
	Outputs                 map[string]json.RawMessage `json:"outputs"`
	IsSystem                bool                       `json:"is_system,omitempty"`
	NodeStatuses            []string                   `json:"node_statuses,omitempty"`
	PausedAtNode            int                        `json:"paused_at_node,omitempty"`
	PauseContext            json.RawMessage            `json:"pause_context,omitempty"`
}

type kexus struct {
	Ops   []Op             `json:"ops"`
	Flows map[string]*Flow `json:"flows"`
}

// FlowCache v3.1: Flow 调试数据缓存结构
type FlowCache struct {
	Outputs      map[string]map[string]json.RawMessage `json:"outputs,omitempty"`       // flow_id -> op_id -> output
	NodeStatuses map[string][]string                   `json:"node_statuses,omitempty"` // flow_id -> []status
}

// ZoneState v3.1: 单个域的完整状态（配置 + 缓存）
type ZoneState struct {
	ZoneID string    // 域标识
	Config kexus     // 配置数据（ops, flows 定义）
	Cache  FlowCache // 调试数据缓存
	Mu     sync.RWMutex // 该域的读写锁
}

// ZoneManager v3.1: 管理所有域的状态
type ZoneManager struct {
	states map[string]*ZoneState // zoneId -> state
	mu     sync.RWMutex          // 保护 states map 本身
}

var zoneManager = &ZoneManager{
	states: make(map[string]*ZoneState),
}

type SSEMessage struct {
	Channel  string `json:"channel"`
	Message  string `json:"message"`
	Priority bool   `json:"-"`
}

type brokerClient struct {
	main     chan SSEMessage
	critical chan SSEMessage
}

type EventBroker struct {
	clients map[*brokerClient]bool
	mu      sync.RWMutex
}

func (b *EventBroker) Subscribe() *brokerClient {
	c := &brokerClient{
		main:     make(chan SSEMessage, 512),
		critical: make(chan SSEMessage, 32),
	}
	b.mu.Lock()
	b.clients[c] = true
	b.mu.Unlock()
	return c
}

func (b *EventBroker) Unsubscribe(c *brokerClient) {
	b.mu.Lock()
	delete(b.clients, c)
	b.mu.Unlock()
	close(c.main)
	close(c.critical)
}

func (b *EventBroker) Broadcast(msg SSEMessage) {
	b.mu.RLock()
	clients := make([]*brokerClient, 0, len(b.clients))
	for c := range b.clients {
		clients = append(clients, c)
	}
	b.mu.RUnlock()

	for _, c := range clients {
		if msg.Priority {
			select {
			case c.main <- msg:
			default:
				select {
				case c.critical <- msg:
				default:
					preview := msg.Message
					if len(preview) > 60 {
						preview = preview[:60]
					}
					log.Printf("⚠️ [SSE] 关键事件丢弃: %s...", preview)
				}
			}
		} else {
			select {
			case c.main <- msg:
			default:
			}
		}
	}
}

var (
	appState      kexus
	flowCache     FlowCache // v3.1: Flow 调试数据缓存
	mu            sync.RWMutex
	saveMu        sync.Mutex
	logFileMutex  sync.Mutex
	broker        = &EventBroker{clients: make(map[*brokerClient]bool)}
	opMutexMatrix sync.Map
	htmlRegex     = regexp.MustCompile(`<[^>]*>`)
	resumeChans   = make(map[string]chan json.RawMessage)
	resumeChansMu sync.Mutex
	// LLM 配置定义（与 .env 对应）
	llmConfigDefs = []struct {
		key    string
		envKey string
	}{
		{"deepseek", "LLM_DEEPSEEK"},
		{"openai", "LLM_OPENAI"},
		{"azure", "LLM_AZURE"},
		{"claude", "LLM_CLAUDE"},
		{"qwen", "LLM_QWEN"},
		{"ernie", "LLM_ERNIE"},
		{"kimi", "LLM_KIMI"},
		{"glm", "LLM_GLM"},
		{"yi", "LLM_YI"},
		{"local", "LLM_LOCAL"},
	}
	// .env 文件热重载监控
	envFilePath      string
	envFileModTime   time.Time
	envFileMu        sync.RWMutex
)

// ==============================================================================
// 🛠️ 核心引擎逻辑
// ==============================================================================

// getEnvOrDefault 读取环境变量，如果不存在返回默认值
func getEnvOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// parseIntOrDefault 解析整数环境变量，失败返回默认值
func parseIntOrDefault(s string, fallback int) int {
	if s == "" {
		return fallback
	}
	if v, err := strconv.Atoi(s); err == nil {
		return v
	}
	return fallback
}

// getActiveLLMConfigs 返回生效的 LLM 配置列表
// 生效规则：API_KEY 不为空 且 BASE_URL 不为空
func getActiveLLMConfigs() []string {
	var active []string
	for _, cfg := range llmConfigDefs {
		apiKey := os.Getenv(cfg.envKey + "_API_KEY")
		baseURL := os.Getenv(cfg.envKey + "_BASE_URL")
		// 特殊处理 local：API_KEY 可以为 not-needed 或任意非空值
		if cfg.key == "local" {
			if baseURL != "" {
				active = append(active, cfg.key)
			}
		} else if apiKey != "" && baseURL != "" {
			active = append(active, cfg.key)
		}
	}
	return active
}

// getEnvFileModTime 获取 .env 文件修改时间
func getEnvFileModTime(path string) (time.Time, error) {
	info, err := os.Stat(path)
	if err != nil {
		return time.Time{}, err
	}
	return info.ModTime(), nil
}

// getRequestZoneId 从请求中获取域ID（优先 query param，其次 cookie）
func getRequestZoneId(r *http.Request) string {
	// 1. 尝试从 query param 获取
	if zone := r.URL.Query().Get("zone"); zone != "" {
		return zone
	}
	
	// 2. 尝试从 cookie 获取
	if cookie, err := r.Cookie("kexus_zone"); err == nil && cookie.Value != "" {
		return cookie.Value
	}
	
	// 3. 返回当前默认域
	return CurrentZone
}

// getZoneState 获取指定域的状态，如果不存在则加载
func getZoneState(zoneId string) *ZoneState {
	if zoneId == "" {
		zoneId = CurrentZone
	}
	
	zoneManager.mu.RLock()
	state, exists := zoneManager.states[zoneId]
	zoneManager.mu.RUnlock()
	
	if exists {
		return state
	}
	
	// 加载新域
	return loadZoneState(zoneId)
}

// loadZoneState 从磁盘加载指定域的配置
func loadZoneState(zoneId string) *ZoneState {
	zonePath := filepath.Join(ZoneDir, zoneId)
	
	// 验证域目录存在
	if _, err := os.Stat(zonePath); os.IsNotExist(err) {
		log.Printf("❌ [zone] 域目录不存在: %s", zonePath)
		return nil
	}
	
	state := &ZoneState{
		ZoneID: zoneId,
		Config: kexus{
			Ops:   []Op{},
			Flows: make(map[string]*Flow),
		},
		Cache: FlowCache{
			Outputs:      make(map[string]map[string]json.RawMessage),
			NodeStatuses: make(map[string][]string),
		},
	}
	
	// 加载 kexus_state.json
	stateFile := filepath.Join(zonePath, ConfigFile)
	if data, err := os.ReadFile(stateFile); err == nil {
		if err := json.Unmarshal(data, &state.Config); err != nil {
			log.Printf("❌ [zone] 解析 %s 配置失败: %v", zoneId, err)
		}
	}
	
	// 加载 kexus_flow_cache.json
	cacheFile := filepath.Join(zonePath, CacheFile)
	if data, err := os.ReadFile(cacheFile); err == nil {
		json.Unmarshal(data, &state.Cache)
	}
	
	// 初始化缺失的字段
	if state.Config.Flows == nil {
		state.Config.Flows = make(map[string]*Flow)
	}
	if state.Config.Ops == nil {
		state.Config.Ops = []Op{}
	}
	if state.Cache.Outputs == nil {
		state.Cache.Outputs = make(map[string]map[string]json.RawMessage)
	}
	if state.Cache.NodeStatuses == nil {
		state.Cache.NodeStatuses = make(map[string][]string)
	}
	
	// 恢复调试数据到 Flow 对象
	for flowID, f := range state.Config.Flows {
		if outputs, ok := state.Cache.Outputs[flowID]; ok {
			f.Outputs = outputs
		}
		if statuses, ok := state.Cache.NodeStatuses[flowID]; ok {
			f.NodeStatuses = statuses
		}
	}
	
	// 保存到 manager
	zoneManager.mu.Lock()
	zoneManager.states[zoneId] = state
	zoneManager.mu.Unlock()
	
	log.Printf("✅ [zone] 已加载域: %s (%d ops, %d flows)", zoneId, len(state.Config.Ops), len(state.Config.Flows))
	return state
}

// saveZoneConfig 保存 Zone 的配置到磁盘
func saveZoneConfig(state *ZoneState) {
	if state == nil {
		return
	}
	
	saveMu.Lock()
	defer saveMu.Unlock()
	
	state.Mu.RLock()
	
	// 创建配置的副本（不包含调试数据）
	stateCopy := kexus{
		Ops:   state.Config.Ops,
		Flows: make(map[string]*Flow),
	}
	
	// 同时提取调试数据
	cacheCopy := FlowCache{
		Outputs:      make(map[string]map[string]json.RawMessage),
		NodeStatuses: make(map[string][]string),
	}
	
	for flowID, f := range state.Config.Flows {
		// 复制 Flow，但不包含 Outputs 和 NodeStatuses
		flowCopy := &Flow{
			ID:                      f.ID,
			Name:                    f.Name,
			Nodes:                   f.Nodes,
			Status:                  f.Status,
			LastError:               f.LastError,
			ScheduleTime:            f.ScheduleTime,
			ScheduleDays:            f.ScheduleDays,
			ScheduleEnabled:         f.ScheduleEnabled,
			ScheduleMode:            f.ScheduleMode,
			ScheduleIntervalHours:   f.ScheduleIntervalHours,
			ScheduleIntervalMinutes: f.ScheduleIntervalMinutes,
			ScheduleIntervalSeconds: f.ScheduleIntervalSeconds,
			Locked:                  f.Locked,
			Private:                 f.Private,
			Meta:                    f.Meta,
			IsSystem:                f.IsSystem,
			PausedAtNode:            f.PausedAtNode,
			PauseContext:            f.PauseContext,
			// 注意：不复制 Outputs 和 NodeStatuses
		}
		stateCopy.Flows[flowID] = flowCopy
		
		// 提取调试数据
		if len(f.Outputs) > 0 {
			cacheCopy.Outputs[flowID] = f.Outputs
		}
		if len(f.NodeStatuses) > 0 {
			cacheCopy.NodeStatuses[flowID] = f.NodeStatuses
		}
	}
	state.Mu.RUnlock()
	
	// 1. 保存主配置文件（不包含调试数据）
	b, err := json.MarshalIndent(stateCopy, "", "  ")
	if err != nil {
		log.Printf("❌ [saveZone] 序列化配置失败: %v", err)
		return
	}
	
	zonePath := filepath.Join(ZoneDir, state.ZoneID)
	stateFile := filepath.Join(zonePath, ConfigFile)
	tmpFile := stateFile + ".tmp"
	if err := os.WriteFile(tmpFile, b, 0644); err != nil {
		log.Printf("❌ [saveZone] 写入配置失败: %v", err)
		return
	}
	if err := os.Rename(tmpFile, stateFile); err != nil {
		log.Printf("❌ [saveZone] 重命名配置失败: %v", err)
		return
	}
	
	// 2. 保存调试数据缓存文件
	cacheFile := filepath.Join(zonePath, CacheFile)
	b, err = json.MarshalIndent(cacheCopy, "", "  ")
	if err != nil {
		log.Printf("❌ [saveZone] 序列化缓存失败: %v", err)
		return
	}
	tmpFile = cacheFile + ".tmp"
	if err := os.WriteFile(tmpFile, b, 0644); err != nil {
		log.Printf("❌ [saveZone] 写入缓存失败: %v", err)
		return
	}
	if err := os.Rename(tmpFile, cacheFile); err != nil {
		log.Printf("❌ [saveZone] 重命名缓存失败: %v", err)
		return
	}
	
	// 3. 更新全局缓存
	state.Mu.Lock()
	state.Cache = cacheCopy
	state.Mu.Unlock()
}

// reloadEnvFile 热重载 .env 文件
func reloadEnvFile() bool {
	envFileMu.Lock()
	defer envFileMu.Unlock()
	
	currentModTime, err := getEnvFileModTime(envFilePath)
	if err != nil {
		return false
	}
	
	// 检查是否变化
	if !currentModTime.After(envFileModTime) {
		return false
	}
	
	// 重新加载配置
	loadEnvMatrix(envFilePath)
	envFileModTime = currentModTime
	
	// 获取更新后的生效配置
	activeConfigs := getActiveLLMConfigs()
	
	broadcastLog(fmt.Sprintf("🔄 [.env] 配置已热重载，生效 LLM: %v", activeConfigs))
	return true
}

// startEnvWatcher 启动 .env 文件监控（轮询模式，每 3 秒检查一次）
func startEnvWatcher(envPath string) {
	envFilePath = envPath
	
	// 初始化修改时间
	modTime, err := getEnvFileModTime(envPath)
	if err != nil {
		log.Printf("⚠️ [.env] 无法获取文件信息: %v", err)
		return
	}
	envFileModTime = modTime
	
	// 启动后台 goroutine 定期轮询
	go func() {
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()
		
		for range ticker.C {
			if reloadEnvFile() {
				// 配置已更新，可以在这里添加额外的回调逻辑
				log.Printf("[.env] 检测到配置变更，已自动重载")
			}
		}
	}()
	
	log.Printf("📡 [.env] 热重载监控已启动 (轮询间隔: 3s)")
}

func loadEnvMatrix(envPath string) bool {
	file, err := os.Open(envPath)
	if err != nil {
		// v3.1: 如果配置文件不存在，返回 false 而不是退出
		log.Printf("⚠️  配置矩阵不存在: %s，使用默认配置", envPath)
		return false
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if len(line) == 0 || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) == 2 {
			os.Setenv(strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1]))
		}
	}
	ProjectDir = os.Getenv("KEXUS_PROJECT_DIR")
	PythonExec = os.Getenv("KEXUS_PYTHON_EXEC")
	if ProjectDir == "" || PythonExec == "" {
		log.Printf("⚠️  核心物理路径未在配置中定义，使用当前目录作为项目目录")
		// v3.1: 使用当前域目录作为默认值
		if ProjectDir == "" {
			ProjectDir = filepath.Dir(envPath)
		}
		if PythonExec == "" {
			PythonExec = "python"
		}
	}

	// v3.0: 加载扩展配置
	ServerPort = getEnvOrDefault("KEXUS_PORT", "1118")
	UseGoEngine = getEnvOrDefault("KEXUS_USE_GO_ENGINE", "false") == "true"
	BackupDir = getEnvOrDefault("KEXUS_BACKUP_DIR", filepath.Join(ProjectDir, "data", ".backup"))
	BackupInterval = parseIntOrDefault(os.Getenv("KEXUS_BACKUP_INTERVAL"), 5)
	BackupRetention = parseIntOrDefault(os.Getenv("KEXUS_BACKUP_RETENTION"), 72)
	FlowTimeout = parseIntOrDefault(os.Getenv("KEXUS_FLOW_TIMEOUT"), 120)
	OpTimeout = parseIntOrDefault(os.Getenv("KEXUS_OP_TIMEOUT"), 600)
	
	return true
}

func getLogFilePath() string {
	return filepath.Join(ProjectDir, "kexus_system.log")
}

func persistLog(msg string) {
	logFileMutex.Lock()
	defer logFileMutex.Unlock()
	cleanMsg := htmlRegex.ReplaceAllString(msg, "")
	f, err := os.OpenFile(getLogFilePath(), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err == nil {
		f.WriteString(time.Now().Format("2006-01-02 ") + cleanMsg + "\n")
		f.Close()
	}
}

// initConfig 加载配置和缓存（v3.1: 分离存储）
func initConfig() {
	appState.Flows = make(map[string]*Flow)
	appState.Ops = []Op{}

	// 1. 加载主配置文件 (kexus_state.json)
	stateFile := filepath.Join(ProjectDir, ConfigFile)
	data, err := os.ReadFile(stateFile)
	if err != nil {
		log.Printf("⚠️ [init] 无法读取状态文件 %s: %v", stateFile, err)
		// 首次运行，使用空配置
		return
	}

	if err := json.Unmarshal(data, &appState); err != nil {
		log.Printf("❌ [init] 解析状态文件失败: %v", err)
		// 备份损坏的文件
		backupFile := stateFile + ".corrupted." + time.Now().Format("20060102_150405")
		os.WriteFile(backupFile, data, 0644)
		log.Printf("💾 [init] 已备份损坏的文件到: %s", backupFile)
		// 使用空配置
		return
	}

	if appState.Flows == nil {
		appState.Flows = make(map[string]*Flow)
	}
	if appState.Ops == nil {
		appState.Ops = []Op{}
	}

	for _, f := range appState.Flows {
		// 清除旧版本遗留的 Stepping 状态，全部重置为 Failed 防坍缩
		if f.Status == "Running" || f.Status == "Paused" || f.Status == "Stepping" {
			f.Status = "Failed"
			f.LastError = "进程重启，执行链已中断"
			f.PausedAtNode = 0
			f.PauseContext = nil
		} else {
			f.Status = "Idle"
		}
		if f.Nodes == nil {
			f.Nodes = []Node{}
		}
		// v3.1: Outputs 和 NodeStatuses 将从缓存文件加载
		if f.ScheduleDays == nil {
			f.ScheduleDays = []int{1, 2, 3, 4, 5, 6, 0}
		}
		if f.ScheduleMode == "" {
			f.ScheduleMode = "daily"
		}
	}

	// 2. 加载缓存文件 (kexus_flow_cache.json)
	loadFlowCache()

	// 3. 迁移旧数据：如果 kexus_state.json 中包含调试数据，提取到缓存
	needsSave := false
	for flowID, f := range appState.Flows {
		if len(f.Outputs) > 0 || len(f.NodeStatuses) > 0 {
			if flowCache.Outputs == nil {
				flowCache.Outputs = make(map[string]map[string]json.RawMessage)
			}
			if flowCache.NodeStatuses == nil {
				flowCache.NodeStatuses = make(map[string][]string)
			}
			if len(f.Outputs) > 0 {
				flowCache.Outputs[flowID] = f.Outputs
			}
			if len(f.NodeStatuses) > 0 {
				flowCache.NodeStatuses[flowID] = f.NodeStatuses
			}
			log.Printf("📦 [migrate] Flow %s 的调试数据已迁移到缓存文件", flowID)
			needsSave = true
		}
	}

	// 4. 将缓存数据恢复到 Flow 对象中
	for flowID, f := range appState.Flows {
		if f.Outputs == nil {
			f.Outputs = make(map[string]json.RawMessage)
		}
		if flowCache.Outputs != nil {
			if outputs, ok := flowCache.Outputs[flowID]; ok {
				f.Outputs = outputs
			}
		}
		if flowCache.NodeStatuses != nil {
			if statuses, ok := flowCache.NodeStatuses[flowID]; ok {
				f.NodeStatuses = statuses
			}
		}
	}

	// v3.1: 跳过保存，避免启动时死锁问题
	// 数据将在第一次需要保存时（如执行 Flow）写入
	if needsSave {
		log.Printf("📦 [init] 检测到需要迁移的调试数据，将在首次保存时写入")
	}

	log.Printf("✅ [init] 配置加载完成: %d ops, %d flows", len(appState.Ops), len(appState.Flows))
}

// loadFlowCache 加载 Flow 调试数据缓存
func loadFlowCache() {
	cacheFile := filepath.Join(ProjectDir, CacheFile)
	data, err := os.ReadFile(cacheFile)
	if err != nil {
		// 缓存文件不存在，初始化空缓存
		flowCache = FlowCache{
			Outputs:      make(map[string]map[string]json.RawMessage),
			NodeStatuses: make(map[string][]string),
		}
		return
	}
	if err := json.Unmarshal(data, &flowCache); err != nil {
		flowCache = FlowCache{
			Outputs:      make(map[string]map[string]json.RawMessage),
			NodeStatuses: make(map[string][]string),
		}
		return
	}
	if flowCache.Outputs == nil {
		flowCache.Outputs = make(map[string]map[string]json.RawMessage)
	}
	if flowCache.NodeStatuses == nil {
		flowCache.NodeStatuses = make(map[string][]string)
	}
}

// saveFlowCache 保存 Flow 调试数据缓存
// 注意：调用者必须持有 mu 读锁或确保没有并发修改
func saveFlowCache() {
	cacheFile := filepath.Join(ProjectDir, CacheFile)

	// 从 Flow 对象中提取调试数据到缓存
	newCache := FlowCache{
		Outputs:      make(map[string]map[string]json.RawMessage),
		NodeStatuses: make(map[string][]string),
	}

	for flowID, f := range appState.Flows {
		if len(f.Outputs) > 0 {
			newCache.Outputs[flowID] = f.Outputs
		}
		if len(f.NodeStatuses) > 0 {
			newCache.NodeStatuses[flowID] = f.NodeStatuses
		}
	}

	b, err := json.MarshalIndent(newCache, "", "  ")
	if err != nil {
		log.Printf("❌ [save] 序列化缓存失败: %v", err)
		return
	}
	tmpFile := cacheFile + ".tmp"
	if err := os.WriteFile(tmpFile, b, 0644); err != nil {
		log.Printf("❌ [save] 写入缓存失败: %v", err)
		return
	}
	if err := os.Rename(tmpFile, cacheFile); err != nil {
		log.Printf("❌ [save] 重命名缓存失败: %v", err)
		return
	}
	
	// 更新全局缓存
	flowCache = newCache
}

// saveConfig 保存配置和缓存（v3.1: 分离存储）
func saveConfig() {
	saveMu.Lock()
	defer saveMu.Unlock()

	mu.RLock()
	// 创建配置的副本（不包含调试数据）
	stateCopy := kexus{
		Ops:   appState.Ops,
		Flows: make(map[string]*Flow),
	}
	
	// 同时提取调试数据
	cacheCopy := FlowCache{
		Outputs:      make(map[string]map[string]json.RawMessage),
		NodeStatuses: make(map[string][]string),
	}
	
	for flowID, f := range appState.Flows {
		// 复制 Flow，但不包含 Outputs 和 NodeStatuses
		flowCopy := &Flow{
			ID:                      f.ID,
			Name:                    f.Name,
			Nodes:                   f.Nodes,
			Status:                  f.Status,
			LastError:               f.LastError,
			ScheduleTime:            f.ScheduleTime,
			ScheduleDays:            f.ScheduleDays,
			ScheduleEnabled:         f.ScheduleEnabled,
			ScheduleMode:            f.ScheduleMode,
			ScheduleIntervalHours:   f.ScheduleIntervalHours,
			ScheduleIntervalMinutes: f.ScheduleIntervalMinutes,
			ScheduleIntervalSeconds: f.ScheduleIntervalSeconds,
			Locked:                  f.Locked,
			Private:                 f.Private,
			Meta:                    f.Meta,
			IsSystem:                f.IsSystem,
			PausedAtNode:            f.PausedAtNode,
			PauseContext:            f.PauseContext,
			// 注意：不复制 Outputs 和 NodeStatuses
		}
		stateCopy.Flows[flowID] = flowCopy
		
		// 提取调试数据
		if len(f.Outputs) > 0 {
			cacheCopy.Outputs[flowID] = f.Outputs
		}
		if len(f.NodeStatuses) > 0 {
			cacheCopy.NodeStatuses[flowID] = f.NodeStatuses
		}
	}
	mu.RUnlock()

	// 1. 保存主配置文件（不包含调试数据）
	b, err := json.MarshalIndent(stateCopy, "", "  ")
	if err != nil {
		log.Printf("❌ [save] 序列化配置失败: %v", err)
		return
	}

	stateFile := filepath.Join(ProjectDir, ConfigFile)
	tmpFile := stateFile + ".tmp"
	if err := os.WriteFile(tmpFile, b, 0644); err != nil {
		log.Printf("❌ [save] 写入配置失败: %v", err)
		return
	}
	if err := os.Rename(tmpFile, stateFile); err != nil {
		log.Printf("❌ [save] 重命名配置失败: %v", err)
		return
	}

	// 2. 保存调试数据缓存文件
	saveFlowCacheWithData(cacheCopy)
}

// saveFlowCacheWithData 使用提供的数据保存缓存
func saveFlowCacheWithData(cache FlowCache) {
	cacheFile := filepath.Join(ProjectDir, CacheFile)

	b, err := json.MarshalIndent(cache, "", "  ")
	if err != nil {
		log.Printf("❌ [save] 序列化缓存失败: %v", err)
		return
	}
	tmpFile := cacheFile + ".tmp"
	if err := os.WriteFile(tmpFile, b, 0644); err != nil {
		log.Printf("❌ [save] 写入缓存失败: %v", err)
		return
	}
	if err := os.Rename(tmpFile, cacheFile); err != nil {
		log.Printf("❌ [save] 重命名缓存失败: %v", err)
		return
	}
	
	// 更新全局缓存
	flowCache = cache
}

func startScheduler() {
	ticker := time.NewTicker(5 * time.Second)
	// lastRunDaily: map[zoneId][flowID]date string "2006-01-02" - for daily schedule mode
	lastRunDaily := make(map[string]map[string]string)
	// lastRunInterval: map[zoneId][flowID]lastRunTime timestamp - for interval schedule mode
	lastRunInterval := make(map[string]map[string]time.Time)

	for range ticker.C {
		now := time.Now()
		currentTime := now.Format("15:04")
		currentDate := now.Format("2006-01-02")
		currentWeekday := int(now.Weekday())
		
		// v3.1: 遍历所有 Zone 的 Flows
		zoneManager.mu.RLock()
		zones := make(map[string]*ZoneState)
		for zoneId, state := range zoneManager.states {
			zones[zoneId] = state
		}
		zoneManager.mu.RUnlock()
		
		for zoneId, state := range zones {
			// 确保每个 Zone 的跟踪映射已初始化
			if lastRunDaily[zoneId] == nil {
				lastRunDaily[zoneId] = make(map[string]string)
			}
			if lastRunInterval[zoneId] == nil {
				lastRunInterval[zoneId] = make(map[string]time.Time)
			}
			
			state.Mu.RLock()
			for id, f := range state.Config.Flows {
				if !f.ScheduleEnabled {
					continue
				}

				scheduleMode := f.ScheduleMode
				if scheduleMode == "" {
					scheduleMode = "daily" // default to daily mode for backward compatibility
				}

				if scheduleMode == "interval" {
					// Interval mode: run every X hours Y minutes Z seconds
					intervalSecs := f.ScheduleIntervalHours*3600 + f.ScheduleIntervalMinutes*60 + f.ScheduleIntervalSeconds
					if intervalSecs <= 0 {
						continue // skip if interval is not set properly
					}

					lastRun, hasLastRun := lastRunInterval[zoneId][id]
					if !hasLastRun || now.Sub(lastRun).Seconds() >= float64(intervalSecs) {
						lastRunInterval[zoneId][id] = now
						// 传递 zoneId 和 flow 信息给 runFlow
						go func(fid, zid, fName string) {
							broadcastLog(fmt.Sprintf("🔄 [循环] 触发 Flow: %s (Zone: %s)", fName, zid))
							runFlow(fid, zid)
						}(id, zoneId, f.Name)
					}
				} else if scheduleMode == "multi_trigger" {
					// 🚀 Multi-trigger mode: multiple time points per day
					for _, trigger := range f.ScheduleTriggers {
						if trigger.Time == currentTime {
							// Check day match
							isDayMatch := false
							for _, d := range trigger.Days {
								if d == currentWeekday {
									isDayMatch = true
									break
								}
							}
							if isDayMatch {
								// Use composite key: flowID_triggerIdx_date
								triggerKey := fmt.Sprintf("%s_%s", id, trigger.Time)
								if lastRunDaily[zoneId][triggerKey] != currentDate {
									lastRunDaily[zoneId][triggerKey] = currentDate
									go func(fid, zid, fName, tTime, tDesc string) {
										desc := tDesc
										if desc == "" {
											desc = tTime
										}
										broadcastLog(fmt.Sprintf("⏰ [多时间点|%s] 触发 Flow: %s (Zone: %s)", desc, fName, zid))
										runFlow(fid, zid)
									}(id, zoneId, f.Name, trigger.Time, trigger.Desc)
								}
							}
						}
					}
				} else {
					// Daily mode: run at specific time on specific days
					if f.ScheduleTime == currentTime {
						isDayMatch := false
						for _, d := range f.ScheduleDays {
							if d == currentWeekday {
								isDayMatch = true
								break
							}
						}
						if isDayMatch && lastRunDaily[zoneId][id] != currentDate {
							lastRunDaily[zoneId][id] = currentDate
							// 传递 zoneId 和 flow 信息给 runFlow
							go func(fid, zid, fName string) {
								broadcastLog(fmt.Sprintf("⏰ [定时器] 触发 Flow: %s (Zone: %s)", fName, zid))
								runFlow(fid, zid)
							}(id, zoneId, f.Name)
						}
					}
				}
			}
			state.Mu.RUnlock()
		}
	}
}

// executePythonOp 执行单个 Python 算子，返回输出和错误
func executePythonOp(targetOp *Op, nodeInputs map[string]interface{}, flowID, flowName string, isSafe bool, state *ZoneState) ([]byte, error) {
	var opLock *sync.Mutex
	if !isSafe {
		actual, _ := opMutexMatrix.LoadOrStore(targetOp.ID, &sync.Mutex{})
		opLock = actual.(*sync.Mutex)
		opLock.Lock()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	cmd := exec.CommandContext(ctx, PythonExec, targetOp.Cmd)
	cmd.Env = append(os.Environ(),
		"PYTHONIOENCODING=utf-8",
		"PYTHONUTF8=1",
		"PYTHONUNBUFFERED=1",  // 🚀 强制 Python 无缓冲输出，确保日志实时显示
		fmt.Sprintf("KEXUS_FLOW_ID=%s", flowID),
		fmt.Sprintf("KEXUS_FLOW_NAME=%s", flowName),
	)
	
	// v3.1: 注入 Zone ID（如果 state 存在）
	if state != nil && state.ZoneID != "" {
		cmd.Env = append(cmd.Env, fmt.Sprintf("KEXUS_ZONE_ID=%s", state.ZoneID))
	}
	
	// 注入算子绑定的 LLM 配置（从独立字段读取）
	if targetOp.LLMBinding != "" {
		cmd.Env = append(cmd.Env, fmt.Sprintf("LLM_BINDING=%s", targetOp.LLMBinding))
	}
	cmd.Dir = ProjectDir

	stdinPipe, _ := cmd.StdinPipe()
	var outBuf bytes.Buffer
	cmd.Stdout = &outBuf
	stderrPipe, _ := cmd.StderrPipe()

	if err := cmd.Start(); err != nil {
		cancel()
		if !isSafe && opLock != nil {
			opLock.Unlock()
		}
		return nil, err
	}

	go func(ni map[string]interface{}) {
		defer stdinPipe.Close()
		payload := make(map[string]interface{})
		for k, v := range ni {
			payload[k] = v
		}
		// v3.1: 使用传入的 Zone state
		if state != nil {
			state.Mu.RLock()
			if lf, ok := state.Config.Flows[flowID]; ok && len(lf.Outputs) > 0 {
				prev := make(map[string]interface{})
				for opID, raw := range lf.Outputs {
					var parsed interface{}
					json.Unmarshal(raw, &parsed)
					prev[opID] = parsed
				}
				payload["__outputs__"] = prev
			}
			state.Mu.RUnlock()
		}
		b, _ := json.Marshal(payload)
		stdinPipe.Write(b)
	}(nodeInputs)

	var wgStream sync.WaitGroup
	wgStream.Add(1)
	go func() { defer wgStream.Done(); streamLog(stderrPipe, false, true) }()

	err := cmd.Wait()
	wgStream.Wait()
	cancel()

	if !isSafe && opLock != nil {
		opLock.Unlock()
	}

	if err != nil {
		return nil, err
	}

	outBytes := bytes.TrimSpace(outBuf.Bytes())
	if bytes.HasPrefix(outBytes, []byte("\xef\xbb\xbf")) {
		outBytes = outBytes[3:]
	}
	return outBytes, nil
}

func runFlow(flowID string, zoneId string) {
	// v3.1: 获取 Zone 状态
	state := getZoneState(zoneId)
	if state == nil {
		broadcastLog(fmt.Sprintf("❌ [系统] Flow %s 执行失败: Zone %s 不存在", flowID, zoneId))
		return
	}
	
	// v3.1: 动态设置 Go 引擎数据目录（支持多域隔离）
	oldDataDir := engine.DataDir
	engine.DataDir = filepath.Join(ProjectDir, "data")
	defer func() {
		engine.DataDir = oldDataDir
	}()
	
	state.Mu.Lock()
	f, exists := state.Config.Flows[flowID]
	if !exists {
		state.Mu.Unlock()
		broadcastLog(fmt.Sprintf("❌ [系统] Flow %s 不存在于 zone %s", flowID, zoneId))
		return
	}
	if f.Status == "Running" {
		state.Mu.Unlock()
		broadcastLog(fmt.Sprintf("⚠️ [系统] Flow %s 已在运行中，忽略重复执行请求", flowID))
		return
	}
	resumeChansMu.Lock()
	if oldChan, had := resumeChans[flowID]; had {
		close(oldChan)
		delete(resumeChans, flowID)
	}
	resumeChansMu.Unlock()

	f.Status = "Running"
	f.LastError = ""
	f.Outputs = make(map[string]json.RawMessage)
	f.PausedAtNode = 0
	f.PauseContext = nil
	nodesSnapshot := make([]Node, len(f.Nodes))
	copy(nodesSnapshot, f.Nodes)
	flowName := f.Name
	f.NodeStatuses = make([]string, len(nodesSnapshot))
	for j := range f.NodeStatuses {
		f.NodeStatuses[j] = "pending"
	}
	state.Mu.Unlock()

	broadcastLog(fmt.Sprintf("🟢 [系统] 开始执行 Flow: %s", flowName))
	broadcastAILog(fmt.Sprintf("🚀 <b>[系统执行]</b> 运行 Flow: <span class='text-[#58a6ff]'>%s</span>", flowName))

	state.Mu.RLock()
	opMap := make(map[string]*Op)
	for i := range state.Config.Ops {
		opMap[state.Config.Ops[i].ID] = &state.Config.Ops[i]
	}
	state.Mu.RUnlock()

	setNodeStatus := func(idx int, status string) {
		state.Mu.Lock()
		if lf, ok := state.Config.Flows[flowID]; ok && idx < len(lf.NodeStatuses) {
			lf.NodeStatuses[idx] = status
		}
		state.Mu.Unlock()
	}

	success := true
	for i, node := range nodesSnapshot {
		// 检查是否已失败需要终止
		if !success {
			break
		}
		targetOp, ok := opMap[node.OpID]
		if !ok {
			msg := fmt.Sprintf("⚠️ [%d/%d] 算子缺失 (ID: %s)，已跳过", i+1, len(nodesSnapshot), node.OpID)
			broadcastLog(msg)
			broadcastAILog("<span class='text-yellow-500'>" + msg + "</span>")
			setNodeStatus(i, "skipped")
			continue
		}
		if !node.Active {
			msg := fmt.Sprintf("⏭️ [%d/%d] 节点未启用，跳过: %s", i+1, len(nodesSnapshot), targetOp.Cmd)
			broadcastLog(msg)
			broadcastAILog("<span class='text-gray-500'>" + msg + "</span>")
			setNodeStatus(i, "skipped")
			continue
		}

		// ============ Go 引擎分流逻辑 ============
		if UseGoEngine && strings.HasPrefix(node.OpID, "op_fetch_") {
			entityName := strings.TrimPrefix(node.OpID, "op_fetch_")

			setNodeStatus(i, "running")
			broadcastLog(fmt.Sprintf("▶️ [%d/%d] 执行算子(Go引擎): %s", i+1, len(nodesSnapshot), targetOp.Cmd))
			broadcastAILog(fmt.Sprintf("⚡ <b>[算子执行-Go]</b> [%d/%d] <span class='text-[#3fb950]'>%s</span>", i+1, len(nodesSnapshot), targetOp.Cmd))

			// 构建 filters
			filters := make(map[string]interface{})
			for k, v := range node.Inputs {
				filters[k] = v
			}

			// 添加 __outputs__ 用于 FK 关联
			state.Mu.RLock()
			if lf, ok := state.Config.Flows[flowID]; ok && len(lf.Outputs) > 0 {
				prev := make(map[string]interface{})
				for opID, raw := range lf.Outputs {
					var parsed interface{}
					json.Unmarshal(raw, &parsed)
					prev[opID] = parsed
				}
				filters["__outputs__"] = prev
			}
			state.Mu.RUnlock()

			// 调用 Go 引擎查询（传入 logger 用于 SSE 日志）
			results, err := engine.ApplyFilters(entityName, filters, func(msg string) {
				broadcastAILog(fmt.Sprintf("⚙️ <span class='text-[#8b949e] font-mono'>%s</span>", msg))
			})
			if err != nil {
				// 读取算子的 on_error 策略
				onError := "abort"
				if targetOp.Meta != nil {
					var metaObj struct {
						OnError string `json:"on_error"`
					}
					if json.Unmarshal(targetOp.Meta, &metaObj) == nil && metaObj.OnError != "" {
						onError = metaObj.OnError
					}
				}

				switch onError {
				case "skip":
					broadcastLog(fmt.Sprintf("⚠️ [容错] Go引擎算子 %s 失败，执行 skip 策略", targetOp.Cmd))
					setNodeStatus(i, "skipped")
					state.Mu.Lock()
					if lf, ok := state.Config.Flows[flowID]; ok {
						lf.LastError = fmt.Sprintf("%s (Go引擎)执行失败(已跳过): %v", targetOp.Cmd, err)
					}
					state.Mu.Unlock()
					continue // 继续下一个节点

				default: // abort
					broadcastCritical(fmt.Sprintf("❌ [Go引擎] 算子 %s 执行失败: %v", targetOp.Cmd, err))
					state.Mu.Lock()
					if lf, ok := state.Config.Flows[flowID]; ok {
						lf.Status = "Failed"
						lf.LastError = fmt.Sprintf("%s (Go引擎): %v", targetOp.Cmd, err)
					}
					state.Mu.Unlock()
					setNodeStatus(i, "failed")
					success = false
					goto FlowEnd
				}
			}

			// 写入 Exchange
			pointer := engine.WriteExchange(node.OpID, results)
			pointerJSON, _ := json.Marshal(pointer)

			// 保存到 Flow.Outputs
			state.Mu.Lock()
			if lf, ok := state.Config.Flows[flowID]; ok {
				if lf.Outputs == nil {
					lf.Outputs = make(map[string]json.RawMessage)
				}
				lf.Outputs[node.OpID] = pointerJSON
			}
			state.Mu.Unlock()

			setNodeStatus(i, "done")
			broadcastLog(fmt.Sprintf("✅ 算子(Go引擎) %s 执行完成, 返回 %d 条记录", targetOp.Cmd, len(results)))
			continue // 跳过 Python 执行
		}
		// ============ 结束 Go 引擎分流 ============

		isSafe := true
		if targetOp.Meta != nil {
			var metaObj struct {
				Safe bool `json:"safe"`
			}
			if err := json.Unmarshal(targetOp.Meta, &metaObj); err == nil {
				isSafe = metaObj.Safe
			}
		}

		setNodeStatus(i, "running")
		
		// 提取算子 LLM 绑定信息用于日志
		llmInfo := ""
		if targetOp.LLMBinding != "" {
			llmInfo = fmt.Sprintf(" [LLM绑定: %s]", targetOp.LLMBinding)
		}
		
		broadcastLog(fmt.Sprintf("▶️ [%d/%d] 执行算子: %s%s", i+1, len(nodesSnapshot), targetOp.Cmd, llmInfo))
		broadcastAILog(fmt.Sprintf("⚡ <b>[算子执行]</b> [%d/%d] <span class='text-[#e6e1cf]'>%s</span>%s", i+1, len(nodesSnapshot), targetOp.Cmd, llmInfo))

		outBytes, err := executePythonOp(targetOp, node.Inputs, flowID, flowName, isSafe, state)
		if err != nil {
			// 执行出错，处理 on_error 策略
			onError := "abort" // 默认终止
			if targetOp.Meta != nil {
				var metaObj struct {
					OnError string `json:"on_error"`
				}
				if json.Unmarshal(targetOp.Meta, &metaObj) == nil && metaObj.OnError != "" {
					onError = metaObj.OnError
				}
			}

			switch onError {
			case "skip":
				broadcastLog(fmt.Sprintf("⚠️ [容错] 算子 %s 失败，执行 skip 策略，继续后续节点", targetOp.Cmd))
				broadcastAILog(fmt.Sprintf("⚠️ <b>[容错 skip]</b> 算子 <span class='text-amber-400'>%s</span> 失败但继续执行", targetOp.Cmd))
				setNodeStatus(i, "skipped")
				state.Mu.Lock()
				if lf, ok := state.Config.Flows[flowID]; ok {
					lf.LastError = fmt.Sprintf("%s 执行失败(已跳过)", targetOp.Cmd)
				}
				state.Mu.Unlock()
				continue

			case "retry:2", "retry:3", "retry:5", "retry:1", "retry:4":
				retryCount := 2
				fmt.Sscanf(onError, "retry:%d", &retryCount)
				broadcastLog(fmt.Sprintf("🔄 [容错] 算子 %s 失败，执行 retry:%d 策略", targetOp.Cmd, retryCount))

				retrySuccess := false
				for r := 0; r < retryCount; r++ {
					broadcastLog(fmt.Sprintf("🔄 [重试] %s 第 %d/%d 次...", targetOp.Cmd, r+1, retryCount))
					time.Sleep(time.Second * time.Duration(r+1)) // 递增延迟
					
					outBytes, err = executePythonOp(targetOp, node.Inputs, flowID, flowName, isSafe, state)
					if err == nil {
						broadcastLog(fmt.Sprintf("✅ [重试成功] 算子 %s 第 %d 次重试成功", targetOp.Cmd, r+1))
						retrySuccess = true
						break
					}
				}

				if !retrySuccess {
					broadcastCritical(fmt.Sprintf("❌ [重试耗尽] 算子 %s 在 %d 次重试后仍失败", targetOp.Cmd, retryCount))
					broadcastAILog(fmt.Sprintf("❌ <b>[重试耗尽]</b> 算子 <span class='text-red-400 font-bold'>%s</span> 失败", targetOp.Cmd))
					state.Mu.Lock()
					if lf, ok := state.Config.Flows[flowID]; ok {
						lf.Status = "Failed"
						lf.LastError = fmt.Sprintf("%s 重试%d次后仍失败", targetOp.Cmd, retryCount)
					}
					state.Mu.Unlock()
					setNodeStatus(i, "failed")
					success = false
					goto FlowEnd
				}
				// 重试成功，继续处理输出（outBytes 已经包含结果）

			default: // "abort" 或其他未识别的策略
				broadcastCritical(fmt.Sprintf("❌ [报错] 算子 %s 执行失败: %v", targetOp.Cmd, err))
				broadcastAILog(fmt.Sprintf("❌ <b>[报错]</b> 算子 <span class='text-red-400 font-bold'>%s</span> 运行失败", targetOp.Cmd))
				state.Mu.Lock()
				if lf, ok := state.Config.Flows[flowID]; ok {
					lf.Status = "Failed"
					lf.LastError = fmt.Sprintf("%s 执行失败", targetOp.Cmd)
				}
				state.Mu.Unlock()
				setNodeStatus(i, "failed")
				success = false
				goto FlowEnd
			}
		}

		// 成功执行后的处理（包括首次成功和重试成功）
		if len(outBytes) == 0 {
			outBytes = []byte("{}")
		}
		if !json.Valid(outBytes) {
			broadcastLog(fmt.Sprintf("⚠️ [物理防线] 算子 %s STDOUT 非 JSON，已强制转义", targetOp.Cmd))
			outBytes, _ = json.Marshal(string(outBytes))
		}

		var outMap map[string]interface{}
		if json.Unmarshal(outBytes, &outMap) == nil {
			if v, ok := outMap["__hitl_pause__"]; ok {
				if isPause, _ := v.(bool); isPause {
					state.Mu.Lock()
					if lf, ok2 := state.Config.Flows[flowID]; ok2 {
						lf.Outputs[targetOp.ID] = outBytes
						lf.Status = "Paused"
						lf.PausedAtNode = i
						lf.PauseContext = outBytes
						if i < len(lf.NodeStatuses) {
							lf.NodeStatuses[i] = "paused"
						}
					}
					state.Mu.Unlock()
					saveZoneConfig(state)

					broadcastLog(fmt.Sprintf("⏸️ [HitL] Flow '%s' 暂停于节点 %d/%d — 等待人工审核", flowName, i+1, len(nodesSnapshot)))
					broadcastAILog(fmt.Sprintf("⏸️ <b>[Human-in-the-Loop]</b> Flow <span class='text-[#d29922]'>%s</span> 等待人工审核", flowName))
					broadcastCritical(fmt.Sprintf("⏸️ [HitL] Flow %s 已暂停", flowName))

					ch := make(chan json.RawMessage, 1)
					resumeChansMu.Lock()
					resumeChans[flowID] = ch
					resumeChansMu.Unlock()

					resumeData, channelOpen := <-ch

					resumeChansMu.Lock()
					delete(resumeChans, flowID)
					resumeChansMu.Unlock()

					if !channelOpen {
						success = false
						goto FlowEnd
					}

					state.Mu.Lock()
					if lf, ok2 := state.Config.Flows[flowID]; ok2 {
						lf.Status = "Running"
						lf.PausedAtNode = 0
						lf.PauseContext = nil
						lf.Outputs[targetOp.ID] = resumeData
						if i < len(lf.NodeStatuses) {
							lf.NodeStatuses[i] = "done"
						}
					}
					state.Mu.Unlock()

					broadcastLog(fmt.Sprintf("▶️ [HitL] Flow '%s' 审核完成，恢复执行", flowName))
					broadcastAILog(fmt.Sprintf("▶️ <b>[HitL]</b> Flow <span class='text-[#3fb950]'>%s</span> 恢复执行", flowName))
					continue
				}
			}
		}

		state.Mu.Lock()
		if lf, ok := state.Config.Flows[flowID]; ok {
			if lf.Outputs == nil {
				lf.Outputs = make(map[string]json.RawMessage)
			}
			lf.Outputs[targetOp.ID] = outBytes
		}
		state.Mu.Unlock()
		setNodeStatus(i, "done")
		broadcastLog(fmt.Sprintf("✅ 算子 %s 执行完成, 输出: %d bytes", targetOp.Cmd, len(outBytes)))
	}

FlowEnd:
	state.Mu.Lock()
	if lf, ok := state.Config.Flows[flowID]; ok {
		if success && lf.Status == "Running" {
			lf.Status = "Success"
			broadcastCritical(fmt.Sprintf("🎉 [完成] Flow %s 执行结束", flowName))
			broadcastAILog(fmt.Sprintf("🎉 <b>[完成]</b> Flow <span class='text-[#58a6ff]'>%s</span> 执行结束", flowName))
		}
	}
	state.Mu.Unlock()
	saveZoneConfig(state)
}

func broadcastLog(msg string) {
	timestamp := time.Now().Format("15:04:05")
	formatted := fmt.Sprintf("[%s] %s", timestamp, msg)
	fmt.Println(formatted)
	broker.Broadcast(SSEMessage{Channel: "sys", Message: formatted})
	persistLog(formatted)
}

func broadcastAILog(msg string) {
	timestamp := time.Now().Format("15:04:05")
	formatted := fmt.Sprintf("<span class='text-gray-500'>[%s]</span> %s", timestamp, msg)
	cleanConsole := htmlRegex.ReplaceAllString(msg, "")
	fmt.Println("[AI] " + cleanConsole)
	broker.Broadcast(SSEMessage{Channel: "ai", Message: formatted})
	persistLog("[AI] [" + timestamp + "] " + cleanConsole)
}

func broadcastCritical(msg string) {
	timestamp := time.Now().Format("15:04:05")
	formatted := fmt.Sprintf("[%s] %s", timestamp, msg)
	fmt.Println(formatted)
	broker.Broadcast(SSEMessage{Channel: "sys", Message: formatted, Priority: true})
	persistLog(formatted)
}

func streamLog(reader io.Reader, isError bool, relayToAI bool) {
	// 🚀 使用行读取 + 大缓冲区，确保日志及时且不截断
	scanner := bufio.NewScanner(reader)
	const maxCapacity = 64 * 1024  // 64KB 缓冲区
	buf := make([]byte, maxCapacity)
	scanner.Buffer(buf, maxCapacity)
	
	for scanner.Scan() {
		text := scanner.Text()
		if strings.TrimSpace(text) != "" {
			if isError {
				broadcastLog(text)  // 直接输出原始内容
				if relayToAI {
					broadcastAILog("<span class='text-red-400'>" + html.EscapeString(text) + "</span>")
				}
			} else {
				broadcastLog(text)  // 直接输出原始内容
				if relayToAI {
					broadcastAILog("<span class='text-[#8b949e]>" + html.EscapeString(text) + "</span>")
				}
			}
		}
	}
}

// ==============================================================================
// 🌐 REST API 路由矩阵
// ==============================================================================

func apiGetState(w http.ResponseWriter, r *http.Request) {
	// 获取请求指定的域
	zoneId := getRequestZoneId(r)
	state := getZoneState(zoneId)
	if state == nil {
		http.Error(w, "Zone not found: "+zoneId, http.StatusNotFound)
		return
	}
	
	state.Mu.RLock()
	// 构建响应（在锁内完成数据拷贝）
	response := struct {
		Ops               []Op                 `json:"ops"`
		Flows             map[string]*Flow     `json:"flows"`
		ActiveLlmConfigs  []string             `json:"active_llm_configs"`
		ZoneId            string               `json:"zone_id"`
	}{
		Ops:              state.Config.Ops,
		Flows:            state.Config.Flows,
		ActiveLlmConfigs: getActiveLLMConfigs(),
		ZoneId:           zoneId,
	}
	state.Mu.RUnlock()
	
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	json.NewEncoder(w).Encode(response)
}

func apiUpdateFlow(w http.ResponseWriter, r *http.Request) {
	var payload Flow
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	
	// v3.1: 获取 Zone 状态
	zoneId := getRequestZoneId(r)
	state := getZoneState(zoneId)
	if state == nil {
		http.Error(w, "Zone not found: "+zoneId, http.StatusNotFound)
		return
	}
	
	state.Mu.Lock()
	if f, ok := state.Config.Flows[payload.ID]; ok {
		if f.Locked && payload.Locked {
			f.ScheduleTime = payload.ScheduleTime
			f.ScheduleDays = payload.ScheduleDays
			f.ScheduleEnabled = payload.ScheduleEnabled
			f.ScheduleMode = payload.ScheduleMode
			f.ScheduleIntervalHours = payload.ScheduleIntervalHours
			f.ScheduleIntervalMinutes = payload.ScheduleIntervalMinutes
			f.ScheduleIntervalSeconds = payload.ScheduleIntervalSeconds
			state.Mu.Unlock()
			saveZoneConfig(state)
			broadcastLog(fmt.Sprintf("🛡️ Flow [%s] 已锁定，仅更新了调度计划", f.Name))
			w.WriteHeader(http.StatusOK)
			return
		}
		f.Name = payload.Name
		f.Nodes = payload.Nodes
		f.ScheduleTime = payload.ScheduleTime
		f.ScheduleDays = payload.ScheduleDays
		f.ScheduleEnabled = payload.ScheduleEnabled
		f.ScheduleMode = payload.ScheduleMode
		f.ScheduleIntervalHours = payload.ScheduleIntervalHours
		f.ScheduleIntervalMinutes = payload.ScheduleIntervalMinutes
		f.ScheduleIntervalSeconds = payload.ScheduleIntervalSeconds
		f.Locked = payload.Locked
		f.Private = payload.Private
		if payload.Meta != nil {
			f.Meta = payload.Meta
		}
		state.Mu.Unlock()
		saveZoneConfig(state)
		broadcastLog(fmt.Sprintf("🔧 Flow [%s] 信息已更新", f.Name))
	} else {
		state.Mu.Unlock()
	}
	w.WriteHeader(http.StatusOK)
}

func apiCreateFlow(w http.ResponseWriter, r *http.Request) {
	var payload struct {
		Name     string          `json:"name"`
		Meta     json.RawMessage `json:"meta"`
		IsSystem bool            `json:"is_system"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	
	// v3.1: 获取 Zone 状态
	zoneId := getRequestZoneId(r)
	state := getZoneState(zoneId)
	if state == nil {
		http.Error(w, "Zone not found: "+zoneId, http.StatusNotFound)
		return
	}
	
	state.Mu.Lock()
	id := fmt.Sprintf("f_%d", time.Now().UnixNano())
	state.Config.Flows[id] = &Flow{
		ID: id, Name: payload.Name, Nodes: []Node{},
		Outputs: make(map[string]json.RawMessage),
		Status:  "Idle", ScheduleTime: "12:00", ScheduleDays: []int{1, 2, 3, 4, 5, 6, 0}, ScheduleEnabled: false,
		ScheduleMode: "daily", ScheduleIntervalHours: 0, ScheduleIntervalMinutes: 5, ScheduleIntervalSeconds: 0,
		Locked: false, Private: false, Meta: payload.Meta,
		IsSystem: payload.IsSystem,
	}
	state.Mu.Unlock()
	saveZoneConfig(state)
	broadcastLog(fmt.Sprintf("✨ 已创建 Flow: [%s]", payload.Name))
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	json.NewEncoder(w).Encode(map[string]string{"id": id})
}

func apiDeleteFlow(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	
	// v3.1: 获取 Zone 状态
	zoneId := getRequestZoneId(r)
	state := getZoneState(zoneId)
	if state == nil {
		http.Error(w, "Zone not found: "+zoneId, http.StatusNotFound)
		return
	}
	
	state.Mu.Lock()
	if f, ok := state.Config.Flows[id]; ok {
		if f.Locked {
			state.Mu.Unlock()
			http.Error(w, "Flow is Locked", http.StatusForbidden)
			return
		}
		broadcastLog(fmt.Sprintf("🗑️ 已删除 Flow: [%s]", f.Name))
		delete(state.Config.Flows, id)
		state.Mu.Unlock()
		saveZoneConfig(state)
	} else {
		state.Mu.Unlock()
	}
	w.WriteHeader(http.StatusOK)
}

func apiUpdateOp(w http.ResponseWriter, r *http.Request) {
	var payload Op
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	
	// v3.1: 获取 Zone 状态
	zoneId := getRequestZoneId(r)
	state := getZoneState(zoneId)
	if state == nil {
		http.Error(w, "Zone not found: "+zoneId, http.StatusNotFound)
		return
	}
	
	state.Mu.Lock()
	for i, o := range state.Config.Ops {
		if o.ID == payload.ID {
			if o.Locked && payload.Locked {
				state.Mu.Unlock()
				http.Error(w, "Op is Locked", http.StatusForbidden)
				return
			}
			state.Config.Ops[i].Name = payload.Name
			state.Config.Ops[i].Locked = payload.Locked
			state.Config.Ops[i].Private = payload.Private
			// 更新 Meta 字段（如果提供了）
			if len(payload.Meta) > 0 {
				state.Config.Ops[i].Meta = payload.Meta
			}
			// 更新 LLM 绑定（允许空字符串表示清除绑定）
			state.Config.Ops[i].LLMBinding = payload.LLMBinding
			state.Mu.Unlock()
			saveZoneConfig(state)
			if payload.LLMBinding != "" {
				broadcastLog(fmt.Sprintf("🏷️ 算子 [%s] 已绑定 LLM: %s", payload.Name, payload.LLMBinding))
			} else {
				broadcastLog(fmt.Sprintf("🏷️ 算子 [%s] 状态已更新", payload.Name))
			}
			w.WriteHeader(http.StatusOK)
			return
		}
	}
	state.Mu.Unlock()
	w.WriteHeader(http.StatusOK)
}

func apiExecuteFlow(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	// v3.1: 获取 Zone 状态并传递给 runFlow
	zoneId := getRequestZoneId(r)
	go runFlow(id, zoneId)
	w.WriteHeader(http.StatusOK)
}

func apiOpSource(w http.ResponseWriter, r *http.Request) {
	cmd := r.URL.Query().Get("cmd")
	if cmd == "" {
		http.Error(w, "cmd is required", http.StatusBadRequest)
		return
	}
	if filepath.Base(cmd) != cmd {
		http.Error(w, "invalid filename: path traversal not allowed", http.StatusBadRequest)
		return
	}
	ext := strings.ToLower(filepath.Ext(cmd))
	if ext != ".py" && ext != ".html" {
		http.Error(w, "only .py and .html files are allowed", http.StatusBadRequest)
		return
	}
	filePath := filepath.Join(ProjectDir, cmd)
	absProject, _ := filepath.Abs(ProjectDir)
	absFile, _ := filepath.Abs(filePath)
	if !strings.HasPrefix(absFile, absProject+string(os.PathSeparator)) {
		http.Error(w, "file path outside project directory", http.StatusForbidden)
		return
	}

	switch r.Method {
	case http.MethodGet:
		content, err := os.ReadFile(filePath)
		if err != nil {
			if os.IsNotExist(err) {
				http.Error(w, "file not found", http.StatusNotFound)
			} else {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
			return
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Write(content)

	case http.MethodPut:
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "read body error: "+err.Error(), http.StatusInternalServerError)
			return
		}
		tmpPath := filePath + ".tmp"
		if err := os.WriteFile(tmpPath, body, 0644); err != nil {
			http.Error(w, "write error: "+err.Error(), http.StatusInternalServerError)
			return
		}
		if err := os.Rename(tmpPath, filePath); err != nil {
			http.Error(w, "rename error: "+err.Error(), http.StatusInternalServerError)
			return
		}
		broadcastLog(fmt.Sprintf("💾 [源码编辑] 已保存: %s (%d bytes)", cmd, len(body)))

		// v3.0: 算子源码修改后触发 KG 自动重编译（仅 .py 文件）
		if ext == ".py" {
			engine.RequestKGRecompile()
		}

		w.WriteHeader(http.StatusOK)

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func apiFlowResume(w http.ResponseWriter, r *http.Request) {
	var payload struct {
		ID   string          `json:"id"`
		Data json.RawMessage `json:"data"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	
	// v3.1: 获取 Zone 状态
	zoneId := getRequestZoneId(r)
	state := getZoneState(zoneId)
	if state == nil {
		http.Error(w, "Zone not found: "+zoneId, http.StatusNotFound)
		return
	}
	
	state.Mu.RLock()
	f, exists := state.Config.Flows[payload.ID]
	isPaused := exists && f.Status == "Paused"
	state.Mu.RUnlock()
	if !isPaused {
		http.Error(w, "flow is not paused", http.StatusConflict)
		return
	}
	resumeData := payload.Data
	if resumeData == nil {
		resumeData = json.RawMessage("{}")
	}
	resumeChansMu.Lock()
	ch, hasChan := resumeChans[payload.ID]
	resumeChansMu.Unlock()
	if !hasChan {
		http.Error(w, "resume channel not ready", http.StatusServiceUnavailable)
		return
	}
	ch <- resumeData
	broadcastLog(fmt.Sprintf("▶️ [HitL] 已向 Flow %s 注入人工审核数据，恢复执行", payload.ID))
	w.WriteHeader(http.StatusOK)
}

func apiFlowAbort(w http.ResponseWriter, r *http.Request) {
	var payload struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	
	// v3.1: 获取 Zone 状态
	zoneId := getRequestZoneId(r)
	state := getZoneState(zoneId)
	if state == nil {
		http.Error(w, "Zone not found: "+zoneId, http.StatusNotFound)
		return
	}
	
	state.Mu.Lock()
	f, exists := state.Config.Flows[payload.ID]
	if !exists {
		state.Mu.Unlock()
		http.Error(w, "flow not found", http.StatusNotFound)
		return
	}
	f.Status = "Failed"
	f.LastError = "用户手动中止"
	for j := range f.NodeStatuses {
		if f.NodeStatuses[j] == "pending" || f.NodeStatuses[j] == "paused" || f.NodeStatuses[j] == "running" {
			f.NodeStatuses[j] = "skipped"
		}
	}
	f.PausedAtNode = 0
	f.PauseContext = nil
	state.Mu.Unlock()
	resumeChansMu.Lock()
	if ch, had := resumeChans[payload.ID]; had {
		close(ch)
		delete(resumeChans, payload.ID)
	}
	resumeChansMu.Unlock()
	saveZoneConfig(state)
	broadcastLog(fmt.Sprintf("⏹️ [中止] Flow %s 已被用户手动中止", payload.ID))
	broadcastCritical(fmt.Sprintf("⏹️ Flow %s 已中止", payload.ID))
	w.WriteHeader(http.StatusOK)
}

// OpInfo 算子信息（v3.0 新增，用于 KG 编译）
// scanOperators 扫描所有算子（提取为公共函数，v3.0）
func scanOperators(projectDir, pythonExec string) ([]engine.OpInfo, error) {
	files, err := os.ReadDir(projectDir)
	if err != nil {
		return nil, err
	}

	scannerCode := `
import ast, sys, json
def run():
    if len(sys.argv) < 3: return
    target_py = sys.argv[1]
    out_json = sys.argv[2]
    try:
        b = open(target_py, 'rb').read()
        if b.startswith(b'\xef\xbb\xbf'): b = b[3:] 
        try:
            c = b.decode('utf-8')
        except UnicodeDecodeError:
            c = b.decode('gbk', 'ignore') 
        
        for node in ast.parse(c).body:
            if isinstance(node, ast.Assign):
                for t in node.targets:
                    if getattr(t, 'id', '') == 'NCD':
                        with open(out_json, 'w', encoding='utf-8') as f:
                            f.write(json.dumps(ast.literal_eval(node.value), ensure_ascii=False))
                        return
    except Exception as e:
        pass
run()
`
	scannerPath := filepath.Join(projectDir, ".kexus_ast_scanner.py")
	os.WriteFile(scannerPath, []byte(scannerCode), 0644)
	defer os.Remove(scannerPath)

	var wg sync.WaitGroup
	var mu sync.Mutex
	var operators []engine.OpInfo

	maxConcurrency := 4
	sem := make(chan struct{}, maxConcurrency)

	for _, f := range files {
		if !f.IsDir() && strings.HasSuffix(f.Name(), ".py") && strings.HasPrefix(f.Name(), "op_") {
			wg.Add(1)
			go func(fileName string) {
				defer wg.Done()
				sem <- struct{}{}
				defer func() { <-sem }()

				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()

				cleanName := strings.TrimSuffix(fileName, ".py")
				id := cleanName

				tmpJsonName := fmt.Sprintf(".meta_tmp_%s.json", id)
				tmpJsonPath := filepath.Join(projectDir, tmpJsonName)
				defer os.Remove(tmpJsonPath)

				probeCmd := exec.CommandContext(ctx, pythonExec, scannerPath, fileName, tmpJsonName)
				probeCmd.Dir = projectDir
				probeCmd.CombinedOutput()

				metaBytes, err := os.ReadFile(tmpJsonPath)
				if err != nil || len(metaBytes) < 5 || !json.Valid(metaBytes) {
					return
				}

				var meta map[string]interface{}
				if err := json.Unmarshal(metaBytes, &meta); err != nil {
					return
				}

				// 提取基本信息
				intent, _ := meta["intent"].(string)
				var tags []string
				if t, ok := meta["tags"].([]interface{}); ok {
					for _, tag := range t {
						if s, ok := tag.(string); ok {
							tags = append(tags, s)
						}
					}
				}

				// 提取 produces_topics / consumes_topics
				var producesTopics, consumesTopics []string
				if pts, ok := meta["produces_topics"].([]interface{}); ok {
					for _, pt := range pts {
						if s, ok := pt.(string); ok {
							producesTopics = append(producesTopics, s)
						}
					}
				}
				if cts, ok := meta["consumes_topics"].([]interface{}); ok {
					for _, ct := range cts {
						if s, ok := ct.(string); ok {
							consumesTopics = append(consumesTopics, s)
						}
					}
				}

				op := engine.OpInfo{
					ID:             id,
					Cmd:            fileName,
					Intent:         intent,
					Tags:           tags,
					Meta:           meta,
					ProducesTopics: producesTopics,
					ConsumesTopics: consumesTopics,
				}

				// 从源码提取 _SCHEMA_* 绑定（用于 KG 编译）
				pyContent, _ := os.ReadFile(filepath.Join(projectDir, fileName))
				if len(pyContent) > 0 {
					// 注意：允许行首有空格（缩进）
					reEntity := regexp.MustCompile(`(?m)^\s*_SCHEMA_ENTITY\s*=\s*["']([^"']+)["']`)
					reVersion := regexp.MustCompile(`(?m)^\s*_SCHEMA_VERSION\s*=\s*["']([^"']+)["']`)
					reHash := regexp.MustCompile(`(?m)^\s*_SCHEMA_HASH\s*=\s*["']([^"']+)["']`)
					text := string(pyContent)
					if m := reEntity.FindStringSubmatch(text); m != nil {
						op.SchemaEntity = m[1]
					}
					if m := reVersion.FindStringSubmatch(text); m != nil {
						op.SchemaVersion = m[1]
					}
					if m := reHash.FindStringSubmatch(text); m != nil {
						op.SchemaHash = m[1]
					}
				}

				mu.Lock()
				operators = append(operators, op)
				mu.Unlock()
			}(f.Name())
		}
	}
	wg.Wait()

	return operators, nil
}

func apiScanOps(w http.ResponseWriter, r *http.Request) {
	// v3.1: 获取 Zone 状态
	zoneId := getRequestZoneId(r)
	state := getZoneState(zoneId)
	if state == nil {
		http.Error(w, "Zone not found: "+zoneId, http.StatusNotFound)
		return
	}
	
	operators, err := scanOperators(ProjectDir, PythonExec)
	if err != nil {
		broadcastLog(fmt.Sprintf("❌ 读取目录失败: %v", err))
		http.Error(w, err.Error(), 500)
		return
	}

	state.Mu.RLock()
	existingMap := make(map[string]bool)
	for _, o := range state.Config.Ops {
		existingMap[o.Cmd] = true
	}
	state.Mu.RUnlock()

	added, updated, rejected := 0, 0, 0

	for _, op := range operators {
		state.Mu.Lock()
		exists := false
		for i, o := range state.Config.Ops {
			if o.Cmd == op.Cmd {
				metaBytes, _ := json.Marshal(op.Meta)
				state.Config.Ops[i].Meta = metaBytes
				state.Config.Ops[i].Tags = op.Tags
				state.Config.Ops[i].Intent = op.Intent
				state.Config.Ops[i].SchemaEntity = op.SchemaEntity
				state.Config.Ops[i].SchemaVersion = op.SchemaVersion
				state.Config.Ops[i].SchemaHash = op.SchemaHash
				state.Config.Ops[i].ProducesTopics = op.ProducesTopics
				state.Config.Ops[i].ConsumesTopics = op.ConsumesTopics
				// 保留用户设置的 LLM 绑定（不覆盖）
				// state.Config.Ops[i].LLMBinding 保持不变
				if state.Config.Ops[i].ID != op.ID {
					state.Config.Ops[i].ID = op.ID
				}
				exists = true
				break
			}
		}

		if !exists {
			metaBytes, _ := json.Marshal(op.Meta)
			state.Config.Ops = append(state.Config.Ops, Op{
				ID:             op.ID,
				Name:           "✨ " + strings.TrimPrefix(op.ID, "op_"),
				Cmd:            op.Cmd,
				Tags:           op.Tags,
				Locked:         false,
				Private:        false,
				Meta:           metaBytes,
				Intent:         op.Intent,
				SchemaEntity:   op.SchemaEntity,
				SchemaVersion:  op.SchemaVersion,
				SchemaHash:     op.SchemaHash,
				ProducesTopics: op.ProducesTopics,
				ConsumesTopics: op.ConsumesTopics,
			})
		}
		state.Mu.Unlock()

		if exists {
			updated++
		} else {
			added++
		}
	}

	// v3.1: 清理已注册但文件不存在的算子（基于目录隔离）
	removed := 0
	state.Mu.Lock()
	newOps := []Op{}
	for _, o := range state.Config.Ops {
		// 检查算子文件是否仍然存在
		cmdPath := filepath.Join(ProjectDir, o.Cmd)
		if _, err := os.Stat(cmdPath); err == nil {
			// 文件存在，保留
			newOps = append(newOps, o)
		} else {
			// 文件不存在，移除
			removed++
			broadcastLog(fmt.Sprintf("🗑️ 移除已消失的算子: %s", o.ID))
		}
	}
	state.Config.Ops = newOps
	state.Mu.Unlock()

	saveZoneConfig(state)

	// v3.0: 触发 KG 自动重编译
	engine.RequestKGRecompile()

	msg := fmt.Sprintf("🔍 扫描完毕：新增 %d 个，更新 %d 个，移除 %d 个，失败 %d 个", added, updated, removed, rejected)
	if added == 0 && updated == 0 && removed == 0 && rejected == 0 {
		msg = "🔍 扫描结束：未发现算子变更。"
	}
	broadcastLog(msg)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"added":    added,
		"updated":  updated,
		"deleted":  removed,  // v3.1: 前端使用 deleted 显示"清除"数量
		"rejected": rejected,
		"message":  msg,
	})
}

func apiDeleteOp(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	
	// v3.1: 获取 Zone 状态
	zoneId := getRequestZoneId(r)
	state := getZoneState(zoneId)
	if state == nil {
		http.Error(w, "Zone not found: "+zoneId, http.StatusNotFound)
		return
	}
	
	state.Mu.Lock()
	newOps := []Op{}
	removedName := ""
	for _, o := range state.Config.Ops {
		if o.ID == id {
			if o.Locked {
				state.Mu.Unlock()
				http.Error(w, "Op is Locked", http.StatusForbidden)
				return
			}
			removedName = o.Cmd
			continue
		}
		newOps = append(newOps, o)
	}
	state.Config.Ops = newOps
	state.Mu.Unlock()

	saveZoneConfig(state)
	if removedName != "" {
		broadcastLog(fmt.Sprintf("🗑️ 已删除算子: [%s]", removedName))
	}
	w.WriteHeader(http.StatusOK)
}

func apiPlannerMeta(w http.ResponseWriter, r *http.Request) {
	// v3.1: 获取 Zone 状态
	zoneId := getRequestZoneId(r)
	state := getZoneState(zoneId)
	if state == nil {
		http.Error(w, "Zone not found: "+zoneId, http.StatusNotFound)
		return
	}
	
	state.Mu.RLock()
	defer state.Mu.RUnlock()
	type PlannerEntity struct {
		ID   string          `json:"id"`
		Type string          `json:"type"`
		Tags []string        `json:"tags,omitempty"`
		Meta json.RawMessage `json:"meta,omitempty"`
	}
	availableEntities := []PlannerEntity{}
	for _, o := range state.Config.Ops {
		if !o.Private {
			availableEntities = append(availableEntities, PlannerEntity{ID: o.ID, Type: "operator", Tags: o.Tags, Meta: o.Meta})
		}
	}
	for _, f := range state.Config.Flows {
		if !f.Private {
			availableEntities = append(availableEntities, PlannerEntity{ID: f.ID, Type: "flow", Meta: f.Meta})
		}
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"system_mode": "Kexus_Planner_View",
		"entities":    availableEntities,
	})
}

func sseLogHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}
	client := broker.Subscribe()
	defer broker.Unsubscribe(client)
	writeMsg := func(msg SSEMessage) {
		b, _ := json.Marshal(msg)
		fmt.Fprintf(w, "data: %s\n\n", string(b))
		flusher.Flush()
	}
	for {
		select {
		case msg := <-client.critical:
			writeMsg(msg)
			continue
		default:
		}
		select {
		case msg := <-client.critical:
			writeMsg(msg)
		case msg := <-client.main:
			writeMsg(msg)
		case <-r.Context().Done():
			return
		}
	}
}

func uiHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
	w.Header().Set("Pragma", "no-cache")
	w.Header().Set("Expires", "0")
	http.ServeFile(w, r, filepath.Join(ProjectDir, "index.html"))
}

func apiRunOp(w http.ResponseWriter, r *http.Request) {
	var payload struct {
		Cmd    string                 `json:"cmd"`
		Inputs map[string]interface{} `json:"inputs"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if payload.Cmd == "" {
		http.Error(w, "cmd is required", http.StatusBadRequest)
		return
	}

	// ============ Go 引擎分流：op_fetch_* 直接由 Go 处理 ============
	cmdName := strings.TrimSuffix(payload.Cmd, ".py")
	if UseGoEngine && strings.HasPrefix(cmdName, "op_fetch_") {
		entityName := strings.TrimPrefix(cmdName, "op_fetch_")
		filters := payload.Inputs
		if filters == nil {
			filters = make(map[string]interface{})
		}

		broadcastLog(fmt.Sprintf("▶️ [apiRunOp] Go引擎处理: %s", payload.Cmd))
		results, err := engine.ApplyFilters(entityName, filters, nil)
		if err != nil {
			http.Error(w, fmt.Sprintf("Go引擎查询失败: %v", err), http.StatusInternalServerError)
			return
		}

		pointer := engine.WriteExchange(cmdName, results)
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		json.NewEncoder(w).Encode(pointer)
		broadcastLog(fmt.Sprintf("✅ [apiRunOp] Go引擎完成: %s, 返回 %d 条记录", payload.Cmd, len(results)))
		return
	}
	// ============ 结束 Go 引擎分流 ============

	inputsJSON, _ := json.Marshal(payload.Inputs)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	cmd := exec.CommandContext(ctx, PythonExec, payload.Cmd)
	cmd.Dir = ProjectDir
	cmd.Env = append(os.Environ(), "PYTHONIOENCODING=utf-8", "PYTHONUTF8=1")
	
	// v3.1: 获取 Zone 状态并注入算子绑定的 LLM 配置
	zoneId := getRequestZoneId(r)
	state := getZoneState(zoneId)
	if state != nil {
		state.Mu.RLock()
		opID := strings.TrimSuffix(payload.Cmd, ".py")
		for _, op := range state.Config.Ops {
			if op.Cmd == payload.Cmd || op.ID == opID {
				if op.LLMBinding != "" {
					cmd.Env = append(cmd.Env, fmt.Sprintf("LLM_BINDING=%s", op.LLMBinding))
				}
				break
			}
		}
		state.Mu.RUnlock()
		// v3.1: 注入 Zone ID
		cmd.Env = append(cmd.Env, fmt.Sprintf("KEXUS_ZONE_ID=%s", state.ZoneID))
	}

	stdinPipe, _ := cmd.StdinPipe()
	var outBuf bytes.Buffer
	cmd.Stdout = &outBuf
	stderrPipe, _ := cmd.StderrPipe()

	if err := cmd.Start(); err != nil {
		http.Error(w, "start error: "+err.Error(), http.StatusInternalServerError)
		return
	}
	go func() {
		defer stdinPipe.Close()
		io.WriteString(stdinPipe, string(inputsJSON))
	}()

	var wgStream sync.WaitGroup
	wgStream.Add(1)
	go func() {
		defer wgStream.Done()
		scanner := bufio.NewScanner(stderrPipe)
		for scanner.Scan() {
			broadcastAILog(fmt.Sprintf("⚙️ <span class='text-[#8b949e] font-mono'>%s</span>", scanner.Text()))
		}
	}()

	cmd.Wait()
	wgStream.Wait()

	outBytes := bytes.TrimSpace(outBuf.Bytes())
	if len(outBytes) == 0 {
		outBytes = []byte("{}")
	}
	if json.Valid(outBytes) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
	} else {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	}
	w.Write(outBytes)
}

func apiOpUI(w http.ResponseWriter, r *http.Request) {
	opID := r.URL.Query().Get("id")
	if opID == "" {
		http.Error(w, "id is required", http.StatusBadRequest)
		return
	}
	
	// v3.1: 获取 Zone 状态
	zoneId := getRequestZoneId(r)
	state := getZoneState(zoneId)
	if state == nil {
		http.Error(w, "Zone not found: "+zoneId, http.StatusNotFound)
		return
	}
	
	state.Mu.RLock()
	var uiFile string
	for _, op := range state.Config.Ops {
		if op.ID == opID && op.Meta != nil {
			var meta struct {
				UI string `json:"ui"`
			}
			if json.Unmarshal(op.Meta, &meta) == nil && meta.UI != "" {
				uiFile = meta.UI
			}
			break
		}
	}
	state.Mu.RUnlock()

	if uiFile == "" {
		http.Error(w, "No UI defined for this operator", http.StatusNotFound)
		return
	}
	uiPath := filepath.Join(ProjectDir, uiFile)
	if _, err := os.Stat(uiPath); os.IsNotExist(err) {
		http.Error(w, "UI file not found: "+uiFile, http.StatusNotFound)
		return
	}
	w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
	w.Header().Set("Pragma", "no-cache")
	w.Header().Set("Expires", "0")
	http.ServeFile(w, r, uiPath)
}

// ==============================================================================
// 🌌 v3.1: 域管理 (Zone Management)
// ==============================================================================

// initZoneManager 初始化域管理器
func initZoneManager() {
	// 获取当前工作目录作为基础目录
	cwd, err := os.Getwd()
	if err != nil {
		log.Fatalf("❌ 无法获取当前工作目录: %v", err)
	}
	BaseProjectDir = cwd
	ZoneDir = filepath.Join(BaseProjectDir, "zone")
	
	// 确保 zone 目录存在
	if err := os.MkdirAll(ZoneDir, 0755); err != nil {
		log.Fatalf("❌ 无法创建 zone 目录: %v", err)
	}
	
	// 检查是否有域切换标记文件
	zoneMarker := filepath.Join(BaseProjectDir, ".current_zone")
	if data, err := os.ReadFile(zoneMarker); err == nil {
		CurrentZone = strings.TrimSpace(string(data))
		// 验证域目录是否存在且已配置（有 .zone 文件）
		zonePath := filepath.Join(ZoneDir, CurrentZone)
		zoneConfigPath := filepath.Join(zonePath, ".zone")
		if _, err := os.Stat(zonePath); err != nil {
			log.Printf("⚠️  标记的域 %s 不存在，使用默认域", CurrentZone)
			CurrentZone = ""
		} else if _, err := os.Stat(zoneConfigPath); err != nil {
			log.Printf("⚠️  标记的域 %s 未配置（缺少 .zone），使用默认域", CurrentZone)
			CurrentZone = ""
		}
	}
	
	// 如果没有激活的域，查找第一个已配置的域（有 .zone 文件）
	if CurrentZone == "" {
		entries, err := os.ReadDir(ZoneDir)
		if err == nil {
			for _, entry := range entries {
				if entry.IsDir() && strings.HasPrefix(entry.Name(), "_") {
					zonePath := filepath.Join(ZoneDir, entry.Name())
					zoneConfigPath := filepath.Join(zonePath, ".zone")
					// 检查是否有 .zone 文件
					if _, err := os.Stat(zoneConfigPath); err == nil {
						CurrentZone = entry.Name()
						break
					}
				}
			}
		}
	}
	
	// 设置项目目录为当前域目录
	if CurrentZone != "" {
		ProjectDir = filepath.Join(ZoneDir, CurrentZone)
		log.Printf("🌌 [zone] 当前域: %s", CurrentZone)
	} else {
		// 如果没有找到任何域，使用基础目录
		ProjectDir = BaseProjectDir
		log.Printf("🌌 [zone] 无域模式，使用基础目录")
	}
}

// getZoneList 获取所有可用域列表
func getZoneList() []map[string]interface{} {
	var zones []map[string]interface{}
	
	entries, err := os.ReadDir(ZoneDir)
	if err != nil {
		return zones
	}
	
	for _, entry := range entries {
		if !entry.IsDir() || !strings.HasPrefix(entry.Name(), "_") {
			continue
		}
		
		zoneId := entry.Name()
		zonePath := filepath.Join(ZoneDir, zoneId)
		
		// 读取域的元数据
		meta := map[string]interface{}{
			"id":       zoneId,
			"name":     zoneId,
			"icon":     "🌌",
			"flows":    0,
			"ops":      0,
			"entities": 0,
		}
		
		// 尝试读取 kexus_state.json 获取统计
		stateFile := filepath.Join(zonePath, "kexus_state.json")
		if data, err := os.ReadFile(stateFile); err == nil {
			var state struct {
				Flows map[string]interface{} `json:"flows"`
				Ops   []interface{}          `json:"ops"`
			}
			if json.Unmarshal(data, &state) == nil {
				meta["flows"] = len(state.Flows)
				meta["ops"] = len(state.Ops)
			}
		}
		
		// 统计实体数量
		metaDir := filepath.Join(zonePath, "meta_schema")
		if entries, err := os.ReadDir(metaDir); err == nil {
			meta["entities"] = len(entries)
		}
		
		zones = append(zones, meta)
	}
	
	return zones
}

// apiZoneList 获取域列表 API
func apiZoneList(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	
	zones := getZoneList()
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status": "success",
		"zones":  zones,
	})
}

// apiZoneCurrent 获取当前域 API
func apiZoneCurrent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":   "success",
		"zone_id":  CurrentZone,
		"zone_dir": ProjectDir,
	})
}

// apiZoneSwitch 切换域 API
func apiZoneSwitch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	
	var payload struct {
		ZoneID string `json:"zone_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	
	// 验证域是否存在
	zonePath := filepath.Join(ZoneDir, payload.ZoneID)
	if _, err := os.Stat(zonePath); err != nil {
		http.Error(w, "Zone not found", http.StatusNotFound)
		return
	}
	
	// 保存切换标记
	CurrentZone = payload.ZoneID
	zoneMarker := filepath.Join(BaseProjectDir, ".current_zone")
	os.WriteFile(zoneMarker, []byte(CurrentZone), 0644)
	
	// 注意：实际的项目目录切换需要重启服务器才能完全生效
	// 这里返回成功，提示用户重启
	broadcastLog(fmt.Sprintf("🌌 [zone] 域切换标记已设置: %s (需要重启服务器生效)", CurrentZone))
	
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":  "success",
		"message": "Domain switch marked. Please restart the server to take effect.",
		"zone_id": CurrentZone,
	})
}

// apiZoneCreate 创建新域 API
func apiZoneCreate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	
	var payload struct {
		ID   string `json:"id"`
		Name string `json:"name"`
		Icon string `json:"icon"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	
	if payload.ID == "" {
		http.Error(w, "Zone ID is required", http.StatusBadRequest)
		return
	}
	
	// 确保ID以下划线开头
	if !strings.HasPrefix(payload.ID, "_") {
		payload.ID = "_" + payload.ID
	}
	
	// 创建域目录
	zonePath := filepath.Join(ZoneDir, payload.ID)
	if _, err := os.Stat(zonePath); err == nil {
		http.Error(w, "Zone already exists", http.StatusConflict)
		return
	}
	
	// 创建目录结构
	dirs := []string{
		zonePath,
		filepath.Join(zonePath, "data"),
		filepath.Join(zonePath, "data", ".exchange"),
		filepath.Join(zonePath, "data", ".backup"),
		filepath.Join(zonePath, "meta_schema"),
		filepath.Join(zonePath, "vendor"),
	}
	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			http.Error(w, fmt.Sprintf("Failed to create directory: %v", err), http.StatusInternalServerError)
			return
		}
	}
	
	// 创建初始 kexus_state.json
	state := map[string]interface{}{
		"ops":   []interface{}{},
		"flows": map[string]interface{}{},
	}
	stateFile := filepath.Join(zonePath, "kexus_state.json")
	stateData, _ := json.MarshalIndent(state, "", "  ")
	os.WriteFile(stateFile, stateData, 0644)
	
	// 创建 .zone 配置文件（zone 目录专属配置）
	zoneConfigContent := fmt.Sprintf(`# Kexus OS Zone Configuration (.zone)
# ⚠️ 此文件为 zone 专属配置，修改后需重启服务生效

# ── 核心路径（相对路径，相对于项目根目录）────────────────────────────────────────
KEXUS_PROJECT_DIR=./zone/%s
KEXUS_PYTHON_EXEC=./kexus_env/Scripts/python.exe
KEXUS_PORT=1118

# ── Zone 元数据 ───────────────────────────────────────────────────────────────
ZONE_ID=%s
ZONE_NAME=%s
ZONE_ICON=%s
`, payload.ID, payload.ID, payload.Name, payload.Icon)
	zoneConfigFile := filepath.Join(zonePath, ".zone")
	os.WriteFile(zoneConfigFile, []byte(zoneConfigContent), 0644)
	
	broadcastLog(fmt.Sprintf("🌌 [zone] 新域已创建: %s", payload.ID))
	
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":  "success",
		"zone_id": payload.ID,
		"message": "Zone created successfully",
	})
}

// zoneHandler 域静态文件服务
func zoneHandler(w http.ResponseWriter, r *http.Request) {
	// 路径格式: /zone/{zoneId}/...
	path := strings.TrimPrefix(r.URL.Path, "/zone/")
	parts := strings.SplitN(path, "/", 2)
	
	if len(parts) == 0 || parts[0] == "" {
		// 如果没有指定域ID，显示域管理首页
		http.ServeFile(w, r, filepath.Join(BaseProjectDir, "zone", "index.html"))
		return
	}
	
	zoneId := parts[0]
	var filePath string
	if len(parts) == 1 || parts[1] == "" {
		// 访问 /zone/{zoneId}/
		filePath = "index.html"
	} else {
		filePath = parts[1]
	}
	
	// 验证域ID
	if !strings.HasPrefix(zoneId, "_") {
		http.Error(w, "Invalid zone ID", http.StatusBadRequest)
		return
	}
	
	// 设置域 cookie（路径为根，便于所有 API 请求携带）
	http.SetCookie(w, &http.Cookie{
		Name:     "kexus_zone",
		Value:    zoneId,
		Path:     "/",
		MaxAge:   86400 * 30, // 30天
		HttpOnly: false,      // 允许 JS 读取
		SameSite: http.SameSiteLaxMode,
	})
	
	// 同时设置当前域（向后兼容）
	CurrentZone = zoneId
	ProjectDir = filepath.Join(ZoneDir, zoneId)
	
	// 构建完整路径
	zonePath := filepath.Join(ZoneDir, zoneId)
	fullPath := filepath.Join(zonePath, filePath)
	
	// 安全检查：确保路径在域目录内
	if !strings.HasPrefix(fullPath, zonePath) {
		http.Error(w, "Access denied", http.StatusForbidden)
		return
	}
	
	// 检查文件是否存在
	if _, err := os.Stat(fullPath); os.IsNotExist(err) {
		http.Error(w, "File not found", http.StatusNotFound)
		return
	}
	
	w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
	w.Header().Set("Pragma", "no-cache")
	w.Header().Set("Expires", "0")
	http.ServeFile(w, r, fullPath)
}

func main() {
	// v3.1: 初始化域管理
	initZoneManager()
	
	// v3.1: 根据运行模式加载对应的环境配置
	// - Zone 模式：加载 zone/{zone_name}/.zone
	// - 根目录模式：加载 .env
	var envPath string
	var hasEnv bool
	if CurrentZone != "" {
		// Zone 模式：加载 .zone 文件
		envPath = filepath.Join(ProjectDir, ".zone")
		hasEnv = loadEnvMatrix(envPath)
		if hasEnv {
			log.Printf("🌌 [zone] 已加载 zone 配置: %s", envPath)
			startEnvWatcher(envPath)
		}
	} else {
		// 根目录模式：加载 .env 文件
		envPath = filepath.Join(ProjectDir, ".env")
		hasEnv = loadEnvMatrix(envPath)
		if hasEnv {
			log.Printf("📁 [root] 已加载根目录配置: %s", envPath)
			startEnvWatcher(envPath)
		}
	}
	
	initConfig()
	initDataEngine() // v3.0: 初始化 Go 数据引擎

	// v3.0: 初始化 Exchange 管理器
	engine.InitExchangeManager(ProjectDir)

	// v3.0: 启动备份管理器
	if BackupInterval > 0 {
		backupDir := filepath.Join(ProjectDir, "data", ".backup")
		busBackup := engine.InitBackupManager(
			filepath.Join(ProjectDir, "data", ".exchange", "kexus_bus.db"),
			backupDir,
			BackupRetention,
			broadcastLog,
		)
		busBackup.StartPeriodicBackup(BackupInterval)

		kgBackup := engine.InitBackupManager(
			filepath.Join(ProjectDir, "data", ".exchange", "kg_topology.db"),
			backupDir,
			BackupRetention,
			broadcastLog,
		)
		// KG 库写入频率低，每小时备份一次
		kgBackup.StartPeriodicBackup(60)

		broadcastLog(fmt.Sprintf("🛡️ [backup] 已启动自动备份: 间隔 %d 分钟, 保留 %d 份", BackupInterval, BackupRetention))
	}

	// v3.0: 启动 KG 自动重编译 worker
	// v3.1: 修改回调函数以支持 Zone，返回当前默认 Zone 的算子
	engine.StartKGRecompileWorker(ProjectDir, func() []engine.OpInfo {
		state := getZoneState(CurrentZone)
		if state == nil {
			return nil
		}
		state.Mu.RLock()
		defer state.Mu.RUnlock()
		ops := make([]engine.OpInfo, len(state.Config.Ops))
		for i, op := range state.Config.Ops {
			// 将 Meta 从 json.RawMessage 转换为 map[string]interface{}
			var metaMap map[string]interface{}
			if op.Meta != nil {
				json.Unmarshal(op.Meta, &metaMap)
			}
			ops[i] = engine.OpInfo{
				ID:             op.ID,
				Cmd:            op.Cmd,
				Intent:         op.Intent,
				Tags:           op.Tags,
				Meta:           metaMap,
				SchemaEntity:   op.SchemaEntity,
				SchemaVersion:  op.SchemaVersion,
				SchemaHash:     op.SchemaHash,
				ProducesTopics: op.ProducesTopics,
				ConsumesTopics: op.ConsumesTopics,
			}
		}
		return ops
	}, broadcastLog)

	// v3.0: 启动 Exchange 定时清理（每10分钟清理一次1小时前的数据）
	engine.StartExchangeCleanup(10)

	go ensureVendorFiles()
	go startScheduler()

	mux := http.NewServeMux()
	
	// v3.1: 根路径根据是否有域重定向
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			// 如果有多个域，显示域管理器
			zones := getZoneList()
			if len(zones) > 1 || CurrentZone == "" {
				http.Redirect(w, r, "/zone/", http.StatusFound)
				return
			}
		}
		uiHandler(w, r)
	})

	mux.HandleFunc("/vendor/", func(w http.ResponseWriter, r *http.Request) {
		name := filepath.Base(r.URL.Path)
		dest := filepath.Join(ProjectDir, "vendor", name)
		if _, err := os.Stat(dest); os.IsNotExist(err) {
			http.Error(w, "vendor file not ready, please wait a moment and refresh", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Cache-Control", "public, max-age=604800")
		http.ServeFile(w, r, dest)
	})

	mux.HandleFunc("/whitepaper", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "kexus_os_v2_whitepaper.html")
	})

	mux.HandleFunc("/op_admin", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
		w.Header().Set("Pragma", "no-cache")
		w.Header().Set("Expires", "0")
		http.ServeFile(w, r, filepath.Join(ProjectDir, "op_admin.html"))
	})

	mux.HandleFunc("/api/state", apiGetState)
	mux.HandleFunc("/api/flow/execute", apiExecuteFlow)
	mux.HandleFunc("/api/flow/update", apiUpdateFlow)
	mux.HandleFunc("/api/flow/create", apiCreateFlow)
	mux.HandleFunc("/api/flow/delete", apiDeleteFlow)
	mux.HandleFunc("/api/flow/resume", apiFlowResume)
	mux.HandleFunc("/api/flow/abort", apiFlowAbort)
	mux.HandleFunc("/api/op/update", apiUpdateOp)
	mux.HandleFunc("/api/op/scan", apiScanOps)
	mux.HandleFunc("/api/op/delete", apiDeleteOp)
	mux.HandleFunc("/api/planner/meta", apiPlannerMeta)
	mux.HandleFunc("/stream", sseLogHandler)

	// 算子执行层
	mux.HandleFunc("/api/op/run", apiRunOp)
	mux.HandleFunc("/api/op/source", apiOpSource)
	mux.HandleFunc("/op-ui", apiOpUI)

	// v3.0: Go Data Engine API
	mux.HandleFunc("/api/engine/query", apiEngineQuery)
	mux.HandleFunc("/api/engine/schema", apiEngineSchema)
	mux.HandleFunc("/api/engine/exchange/write", apiEngineExchangeWrite)
	mux.HandleFunc("/api/engine/exchange/read", apiEngineExchangeRead)
	mux.HandleFunc("/api/engine/evolve", apiEngineEvolve)
	mux.HandleFunc("/api/engine/insert", apiEngineInsert)
	mux.HandleFunc("/api/engine/kg/compile", apiEngineKGCompile)
	mux.HandleFunc("/api/engine/kg/query", apiEngineKGQuery)
	mux.HandleFunc("/api/engine/init", apiEngineInit)

	// v3.1: 域管理 API
	mux.HandleFunc("/api/zone/list", apiZoneList)
	mux.HandleFunc("/api/zone/current", apiZoneCurrent)
	mux.HandleFunc("/api/zone/switch", apiZoneSwitch)
	mux.HandleFunc("/api/zone/create", apiZoneCreate)
	
	// v3.1: 域静态文件服务
	mux.HandleFunc("/zone/", zoneHandler)

	handler := cors.Default().Handler(mux)

	addr := ":" + ServerPort
	fmt.Println("▅▅▅▅▅▅▅▅▅▅▅▅▅▅▅▅▅▅▅▅▅▅▅▅▅▅▅▅▅▅▅▅▅▅▅▅▅▅▅▅▅▅▅▅▅▅▅▅▅▅▅▅▅")
	fmt.Println("👑 Kexus OS v3.0 已启动")
	fmt.Printf("📂 工作目录: %s\n", ProjectDir)
	fmt.Printf("📡 监听端口: %s\n", addr)
	fmt.Printf("👉 GUI 入口: http://localhost:%s\n", ServerPort)
	fmt.Printf("🚀 Go数据引擎: %v\n", UseGoEngine)
	fmt.Println("▅▅▅▅▅▅▅▅▅▅▅▅▅▅▅▅▅▅▅▅▅▅▅▅▅▅▅▅▅▅▅▅▅▅▅▅▅▅▅▅▅▅▅▅▅▅▅▅▅▅▅▅▅\n")

	log.Fatal(http.ListenAndServe(addr, handler))
}

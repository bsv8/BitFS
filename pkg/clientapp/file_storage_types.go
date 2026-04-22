package clientapp

// 文件存储表记录。
// 这里保留的是运行时和测试共用的数据壳，不再挂任何旧 workspace 管理动作。
type workspaceItem struct {
	WorkspacePath string `json:"workspace_path"`
	MaxBytes      uint64 `json:"max_bytes"`
	Enabled       bool   `json:"enabled"`
	CreatedAtUnix int64  `json:"created_at_unix"`
}

// 下载完成后登记所需的最小输入。
type registerDownloadedFileParams struct {
	FilePath              string
	Seed                  []byte
	AvailableChunkIndexes []uint32
	RecommendedFileName   string
	MIMEHint              string
}

// 直播缓存流的统计信息。
type liveCacheStreamStat struct {
	StreamID            string
	TotalBytes          uint64
	NewestUpdatedAtUnix int64
	Paths               []string
	WorkspaceDirs       []string
}

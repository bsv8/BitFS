package filedownload

import "time"

// Job 状态值定义
const (
	StateQueued           = "queued"
	StateLocal            = "local"
	StateRunning          = "running"
	StateBlockedByBudget  = "blocked_by_budget"
	StateQuoteUnavailable = "quote_unavailable"
	StateQuoteTimeout     = "quote_timeout"
	StateFailed           = "failed"
	StateDone             = "done"
)

// Job 下载任务模型（DB-backed store 用 struct tag，生产模块内部用）
type Job struct {
	JobID           string
	SeedHash        string
	DemandID        string
	State           string
	ChunkCount      uint32
	CompletedChunks uint32
	PaidTotalSat    uint64
	OutputFilePath  string
	PartFilePath    string
	Error           string
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

// Chunk 状态值定义
const (
	ChunkStatePending     = "pending"
	ChunkStateDownloading = "downloading"
	ChunkStateStored      = "stored"
	ChunkStateFailed      = "failed"
)

// StartRequest 下载入口入参
type StartRequest struct {
	SeedHash      string `json:"seed_hash"`
	ChunkCount    uint32 `json:"chunk_count,omitempty"`
	GatewayPubkey string `json:"gateway_pubkey_hex,omitempty"`
	MaxSeedPrice  uint64 `json:"max_seed_price_sat,omitempty"`
	MaxChunkPrice uint64 `json:"max_chunk_price_sat,omitempty"`
}

// StartResult 下载入口返回
type StartResult struct {
	JobID   string `json:"job_id"`
	Status  Status `json:"status"`
	Message string `json:"message,omitempty"`
}

// Status job 状态
type Status struct {
	JobID           string `json:"job_id"`
	SeedHash        string `json:"seed_hash"`
	DemandID        string `json:"demand_id,omitempty"`
	State           string `json:"state"`
	ChunkCount      uint32 `json:"chunk_count"`
	CompletedChunks uint32 `json:"completed_chunks"`
	PaidTotalSat    uint64 `json:"paid_total_sat"`
	OutputFilePath  string `json:"output_file_path,omitempty"`
	PartFilePath    string `json:"part_file_path,omitempty"`
	Error           string `json:"error,omitempty"`
	CreatedAtUnix   int64  `json:"created_at_unix"`
	UpdatedAtUnix   int64  `json:"updated_at_unix"`
}

// Chunk 存储模型
type Chunk struct {
	Index        uint32 `json:"index"`
	Hash         string `json:"hash"`
	State        string `json:"state"`
	Bytes        []byte `json:"bytes,omitempty"`
	PriceSat     uint64 `json:"price_sat"`
	SellerPubkey string `json:"seller_pubkey_hex,omitempty"`
}

// ChunkReport 单 chunk 上报（用于事实记录）
type ChunkReport struct {
	ChunkIndex    uint32 `json:"chunk_index"`
	State         string `json:"state,omitempty"`
	SellerPubkey  string `json:"seller_pubkey_hex"`
	ChunkPriceSat uint64 `json:"chunk_price_sat"`
	SpeedBps      uint64 `json:"speed_bps,omitempty"`
	Selected      bool   `json:"selected"`
	Error         string `json:"error,omitempty"`
	RejectReason  string `json:"reject_reason,omitempty"`
}

// NodeReport 单 seller/node 汇总（从 chunk facts 自动聚合，不作为主事实存储）
type NodeReport struct {
	SellerPubkey       string        `json:"seller_pubkey_hex"`
	Chunks             []ChunkReport `json:"chunks"`
	TotalPaidSat       uint64        `json:"total_paid_sat"`
	AvgSpeedBps        uint64        `json:"avg_speed_bps"`
	SelectedCount      uint32        `json:"selected_count"`
	RejectedCount      uint32        `json:"rejected_count"`
	ReportedChunkCount uint32        `json:"reported_chunk_count"`
}

// QuoteReport 单 seller 报价
type QuoteReport struct {
	SellerPubkey        string   `json:"seller_pubkey_hex"`
	SeedPriceSat        uint64   `json:"seed_price_sat"`
	ChunkPriceSat       uint64   `json:"chunk_price_sat"`
	ChunkCount          uint32   `json:"chunk_count"`
	AvailableChunks     []uint32 `json:"available_chunks,omitempty"`
	RecommendedFileName string   `json:"recommended_file_name,omitempty"`
	MimeType            string   `json:"mime_type,omitempty"`
	FileSizeBytes       uint64   `json:"file_size_bytes,omitempty"`
	QuoteTimestamp      int64    `json:"quote_timestamp"`
	ExpiresAtUnix       int64    `json:"expires_at_unix,omitempty"`
	Selected            bool     `json:"selected"`
	RejectReason        string   `json:"reject_reason,omitempty"`
}

// SeedMeta seed 元数据（不含实际 bytes）
type SeedMeta struct {
	SeedHash      string   `json:"seed_hash"`
	ChunkHashes   []string `json:"chunk_hashes"`
	ChunkCount    uint32   `json:"chunk_count"`
	FileSize      uint64   `json:"file_size"`
	CreatedAtUnix int64    `json:"created_at_unix"`
}

// SaveSeedInput 保存 seed 输入
type SaveSeedInput struct {
	SeedHash    string
	SeedBytes   []byte
	ChunkHashes []string
	ChunkCount  uint32
	FileSize    uint64
}

// LocalFile 本地文件信息
type LocalFile struct {
	FilePath string `json:"file_path"`
	FileSize uint64 `json:"file_size"`
	SeedHash string `json:"seed_hash"`
	MimeType string `json:"mime_type,omitempty"`
}

// PartFile 局部文件信息
type PartFile struct {
	PartFilePath string `json:"part_file_path"`
	SeedHash     string `json:"seed_hash"`
}

// PreparePartFileInput 准备 part 文件输入
type PreparePartFileInput struct {
	SeedHash string
	FileSize uint64
}

// StoredChunkInput chunk 已存储输入
type StoredChunkInput struct {
	SeedHash   string
	ChunkIndex uint32
	ChunkBytes []byte
}

// CompleteFileInput 完成文件输入
type CompleteFileInput struct {
	SeedHash              string
	PartFilePath          string
	RecommendedFileName   string
	MimeType              string
	AvailableChunkIndexes []uint32
}

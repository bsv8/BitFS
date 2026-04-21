package filedownload

import (
	"context"
)

// SeedStore seed 存储能力（只管 seed bytes、seed_hash、chunk_hashes、chunk_count、file_size）
type SeedStore interface {
	// SaveSeed 保存 seed 元数据和 bytes
	SaveSeed(ctx context.Context, input SaveSeedInput) error
	// LoadSeedMeta 加载 seed 元数据（不含 bytes）
	LoadSeedMeta(ctx context.Context, seedHash string) (SeedMeta, bool, error)
}

// FileStore 本地文件存储能力（只管本地文件、part 文件、chunk 落盘、最终文件登记）
type FileStore interface {
	// FindCompleteFile 查找本地是否有完整的文件
	FindCompleteFile(ctx context.Context, seedHash string) (LocalFile, bool, error)
	// PreparePartFile 准备 part 文件（用于分块下载）
	PreparePartFile(ctx context.Context, input PreparePartFileInput) (PartFile, error)
	// MarkChunkStored 标记 chunk 已存储
	MarkChunkStored(ctx context.Context, input StoredChunkInput) error
	// CompleteFile 完成文件（将 part 文件合成为完整文件并登记）
	CompleteFile(ctx context.Context, input CompleteFileInput) (LocalFile, error)
}

// DemandPublisher 发布需求能力
type DemandPublisher interface {
	// PublishDemand 发布下载需求
	PublishDemand(ctx context.Context, req PublishDemandRequest) (PublishDemandResult, error)
}

// PublishDemandRequest 发布需求请求
type PublishDemandRequest struct {
	SeedHash   string
	ChunkCount uint32
	GatewayID  string
}

// PublishDemandResult 发布需求结果
type PublishDemandResult struct {
	DemandID string
	Status   string
}

// QuoteReader 读取报价能力
type QuoteReader interface {
	// ListQuotes 列出某 demand 的所有报价
	ListQuotes(ctx context.Context, demandID string) ([]QuoteReport, error)
}

// TransferRunner 传输执行能力
type TransferRunner interface {
	// RunChunkTransfer 执行单 chunk 传输
	RunChunkTransfer(ctx context.Context, req ChunkTransferRequest) (ChunkTransferResult, error)
}

// ChunkTransferRequest chunk 传输请求
type ChunkTransferRequest struct {
	DemandID      string
	SeedHash      string
	ChunkHash     string
	SellerPubkey  string
	ChunkIndex    uint32
	ChunkPriceSat uint64
	MaxRetries    int
}

// ChunkTransferResult chunk 传输结果
type ChunkTransferResult struct {
	Data         []byte
	PaidSat      uint64
	SpeedBps     uint64
	RejectReason string
}

// DownloadPolicy 下载策略能力
type DownloadPolicy interface {
	// SelectQuote 选择本次下载使用的报价。
	// 返回值：
	//   - selectedQuote: 被选中的报价
	//   - ok: true 表示找到可用报价
	//   - reason: 失败或拒绝原因
	SelectQuote(ctx context.Context, req StartRequest, quotes []QuoteReport) (selectedQuote QuoteReport, ok bool, reason string, err error)
}

// DownloadCaps 静态下载总调度能力集合。
// 这里只允许放行为接口，不允许放具体实现对象。
type DownloadCaps struct {
	Jobs      JobStore
	Seeds     SeedStore
	Files     FileStore
	Demands   DemandPublisher
	Quotes    QuoteReader
	Policy    DownloadPolicy
	Transfers TransferRunner
}

// JobStore job 存储能力（持久化语义）
type JobStore interface {
	// CreateJob 创建新 job 或返回已有 job（seed_hash 去重）。
	// 返回值 (job, created, error)：
	//   - job: 实际使用的 job（含 DB 中的完整数据）
	//   - created: true=本次新建，false=复用已有 job
	//   - error: 创建失败（不含 seed_hash 冲突，冲突时返回已有 job + created=false）
	CreateJob(ctx context.Context, job *Job) (Job, bool, error)
	// GetJob 读取 job 快照
	GetJob(ctx context.Context, jobID string) (Job, bool)
	// FindJobBySeedHash 按 seed_hash 查找已有 job（用于去重）
	FindJobBySeedHash(ctx context.Context, seedHash string) (Job, bool, error)
	// UpdateJobState 更新 job 状态
	UpdateJobState(ctx context.Context, jobID string, state string) error
	// SetChunkCount 设置 job 的 chunk 总数
	SetChunkCount(ctx context.Context, jobID string, chunkCount uint32) error
	// SetDemandID 设置 demand ID
	SetDemandID(ctx context.Context, jobID string, demandID string) error
	// SetError 设置 job 错误信息
	SetError(ctx context.Context, jobID string, message string) error
	// SetPartFilePath 设置 part 文件路径
	SetPartFilePath(ctx context.Context, jobID string, partFilePath string) error
	// AppendQuote 追加 seller 报价
	AppendQuote(ctx context.Context, jobID string, quote QuoteReport) error
	// AppendChunkReport 追加 chunk 上报
	AppendChunkReport(ctx context.Context, jobID string, report ChunkReport) error
	// SetOutputPath 设置输出路径
	SetOutputPath(ctx context.Context, jobID string, path string) error
	// ListChunks 读取 job 的 chunk 上报
	ListChunks(ctx context.Context, jobID string) ([]ChunkReport, bool)
	// ListQuotes 读取 job 的 quotes
	ListQuotes(ctx context.Context, jobID string) ([]QuoteReport, bool)
	// ListJobs 列出所有 job 快照（只读查询）
	ListJobs(ctx context.Context) []Job
}

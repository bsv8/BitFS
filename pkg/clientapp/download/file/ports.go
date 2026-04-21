package filedownload

import (
	"context"
	"time"
)

// SeedStore seed 存储能力（只管 seed bytes、seed_hash、chunk_hashes、chunk_count、file_size）
type SeedStore interface {
	// SaveSeed 保存 seed 元数据和 bytes
	SaveSeed(ctx context.Context, input SaveSeedInput) error
	// LoadSeedMeta 加载 seed 元数据（不含 bytes）
	LoadSeedMeta(ctx context.Context, seedHash string) (SeedMeta, bool, error)
}

// SeedResolver seed 解析能力。
// 设计说明：
// - 本地没有 seed meta 时，必须能通过卖家侧补回并落盘；
// - 解析结果不是临时值，必须写回 SeedStore 或等价事实层。
type SeedResolver interface {
	ResolveAndSaveSeedMeta(ctx context.Context, req SeedResolveRequest) (SeedMeta, error)
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

// FrontOrderBinder 下载级 front_order 绑定能力。
// 设计说明：
// - 只负责确保本次下载对应的 front_order，以及 job 与 front_order 的绑定；
// - 不承载 business / settlement 实现细节；
// - 已绑定后不得改绑到别的 front_order。
type FrontOrderBinder interface {
	EnsureFrontOrder(ctx context.Context, req FrontOrderRequest) (string, error)
	BindJobFrontOrder(ctx context.Context, jobID string, frontOrderID string) error
}

// FrontOrderRequest 绑定下载级 front_order 的请求。
type FrontOrderRequest struct {
	JobID          string
	SeedHash       string
	FrontOrderID   string
	Note           string
	OwnerPubkeyHex string
	TargetObjectID string
}

// DemandPublisher 发布需求能力
type DemandPublisher interface {
	// PublishDemand 发布下载需求
	PublishDemand(ctx context.Context, req PublishDemandRequest) (PublishDemandResult, error)
}

// PublishDemandRequest 发布需求请求
type PublishDemandRequest struct {
	JobID        string
	FrontOrderID string
	SeedHash     string
	ChunkCount   uint32
	GatewayID    string
}

// PublishDemandResult 发布需求结果
type PublishDemandResult struct {
	DemandID string
	Status   string
}

// QuoteReader 读取报价能力。
type QuoteReader interface {
	// ListQuotes 列出某 demand 的所有报价
	ListQuotes(ctx context.Context, demandID string) ([]QuoteReport, error)
}

// QuoteWaiter 等待报价能力。
type QuoteWaiter interface {
	WaitQuotes(ctx context.Context, req QuoteWaitRequest) ([]QuoteReport, error)
}

// QuoteWaitRequest 等待报价请求。
type QuoteWaitRequest struct {
	DemandID         string
	MaxRetry         int
	Interval         time.Duration
	MaxSeedPriceSat  uint64
	MaxChunkPriceSat uint64
}

// StrategyTransferRunner 多卖家策略传输能力。
type StrategyTransferRunner interface {
	RunTransferByStrategy(ctx context.Context, req StrategyTransferRequest) (StrategyTransferResult, error)
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

// StrategyTransferRequest 多卖家策略传输请求。
type StrategyTransferRequest struct {
	FrontOrderID    string
	DemandID        string
	SeedHash        string
	ChunkCount      uint32
	ArbiterPubHex   string
	MaxSeedPrice    uint64
	MaxChunkPrice   uint64
	Strategy        string
	PoolAmountSat   uint64
	MaxChunkRetries int
	Candidates      []QuoteReport
}

// StrategyTransferResult 多卖家策略传输结果。
type StrategyTransferResult struct {
	ChunkCount uint32
	Chunks     []TransferChunkResult
	Sellers    []TransferSellerStatItem
}

// DownloadPolicy 下载策略能力
type DownloadPolicy interface {
	// SelectQuotes 选择本次下载使用的报价集合。
	SelectQuotes(ctx context.Context, req StartRequest, quotes []QuoteReport) (QuoteSelection, error)
}

// DownloadCaps 静态下载总调度能力集合。
// 这里只允许放行为接口，不允许放具体实现对象。
type DownloadCaps struct {
	Jobs         JobStore
	FrontOrders  FrontOrderBinder
	Seeds        SeedStore
	SeedResolver SeedResolver
	Files        FileStore
	Demands      DemandPublisher
	Quotes       QuoteReader
	QuoteWaiter  QuoteWaiter
	Policy       DownloadPolicy
	Transfers    StrategyTransferRunner
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
	// SetFrontOrderID 设置下载级 front_order_id，已绑定时不允许改绑到别的值
	SetFrontOrderID(ctx context.Context, jobID string, frontOrderID string) error
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

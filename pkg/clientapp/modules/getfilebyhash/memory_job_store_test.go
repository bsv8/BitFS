package getfilebyhash

import (
	"context"
	"sync"
	"time"
)

// memoryJob 是 MemoryJobStore 内部使用的 job 扩展版本（带运行时字段）。
type memoryJob struct {
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

	chunkReports    map[uint32]ChunkReport
	quotes          map[string]QuoteReport
	completedChunks map[uint32]bool
	mu              sync.RWMutex
}

// MemoryJobStore 内存 job store 实现
//
// 注意：此实现仅供测试使用。生产环境必须使用持久化 DB store。
// 运行入口不应直接使用此实现。
type MemoryJobStore struct {
	jobs map[string]*memoryJob
	mu   sync.RWMutex
}

// NewMemoryJobStoreForTest 创建仅供测试使用的内存 store
//
// 注意：此函数返回的 store 仅用于单元测试和集成测试。
// 生产代码接线时应拒绝使用此实现，确保使用持久化存储。
func NewMemoryJobStoreForTest() *MemoryJobStore {
	return &MemoryJobStore{
		jobs: make(map[string]*memoryJob),
	}
}

// jobToStatus 将 memoryJob 转换为 Status
func jobToStatus(job *memoryJob) Status {
	return Status{
		JobID:           job.JobID,
		SeedHash:        job.SeedHash,
		DemandID:        job.DemandID,
		State:           job.State,
		ChunkCount:      job.ChunkCount,
		CompletedChunks: job.CompletedChunks,
		PaidTotalSat:    job.PaidTotalSat,
		OutputFilePath:  job.OutputFilePath,
		PartFilePath:    job.PartFilePath,
		Error:           job.Error,
		CreatedAtUnix:   job.CreatedAt.Unix(),
		UpdatedAtUnix:   job.UpdatedAt.Unix(),
	}
}

// jobToSnapshot 将 memoryJob 转换为 Job 快照
func jobToSnapshot(job *memoryJob) Job {
	job.mu.RLock()
	defer job.mu.RUnlock()

	return Job{
		JobID:           job.JobID,
		SeedHash:        job.SeedHash,
		DemandID:        job.DemandID,
		State:           job.State,
		ChunkCount:      job.ChunkCount,
		CompletedChunks: job.CompletedChunks,
		PaidTotalSat:    job.PaidTotalSat,
		OutputFilePath:  job.OutputFilePath,
		PartFilePath:    job.PartFilePath,
		Error:           job.Error,
		CreatedAt:       job.CreatedAt,
		UpdatedAt:       job.UpdatedAt,
	}
}

// CreateJob 创建新 job
func (s *MemoryJobStore) CreateJob(ctx context.Context, job *Job) error {
	if job == nil {
		return NewError(CodeBadRequest, "job is nil")
	}
	if job.JobID == "" {
		return NewError(CodeBadRequest, "job_id is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.jobs[job.JobID]; exists {
		return NewError(CodeBadRequest, "job already exists: "+job.JobID)
	}

	mj := &memoryJob{
		JobID:      job.JobID,
		SeedHash:   job.SeedHash,
		DemandID:   job.DemandID,
		State:      job.State,
		ChunkCount: job.ChunkCount,
	}
	mj.chunkReports = make(map[uint32]ChunkReport)
	mj.quotes = make(map[string]QuoteReport)
	mj.completedChunks = make(map[uint32]bool)
	mj.CreatedAt = time.Now()
	mj.UpdatedAt = time.Now()
	s.jobs[job.JobID] = mj
	return nil
}

// GetJob 读取 job 快照（不暴露内部指针）
func (s *MemoryJobStore) GetJob(ctx context.Context, jobID string) (Job, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	job, ok := s.jobs[jobID]
	if !ok {
		return Job{}, false
	}
	snapshot := jobToSnapshot(job)
	return snapshot, true
}

// FindJobBySeedHash 按 seed_hash 查找已有 job（用于去重）
// 返回所有状态的可复用 job，包括 done/local/failed（允许同一 seed 已有 job 时不再创建新 job）
func (s *MemoryJobStore) FindJobBySeedHash(ctx context.Context, seedHash string) (Job, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, job := range s.jobs {
		if job.SeedHash == seedHash {
			snapshot := jobToSnapshot(job)
			return snapshot, true, nil
		}
	}
	return Job{}, false, nil
}

// UpdateJobState 更新 job 状态
func (s *MemoryJobStore) UpdateJobState(ctx context.Context, jobID string, state string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	job, ok := s.jobs[jobID]
	if !ok {
		return NewError(CodeJobNotFound, "job not found: "+jobID)
	}
	job.State = state
	job.UpdatedAt = time.Now()
	return nil
}

// SetDemandID 设置 demand ID
func (s *MemoryJobStore) SetDemandID(ctx context.Context, jobID string, demandID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	job, ok := s.jobs[jobID]
	if !ok {
		return NewError(CodeJobNotFound, "job not found: "+jobID)
	}
	job.DemandID = demandID
	job.UpdatedAt = time.Now()
	return nil
}

// SetPartFilePath 设置 part 文件路径
func (s *MemoryJobStore) SetPartFilePath(ctx context.Context, jobID string, partFilePath string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	job, ok := s.jobs[jobID]
	if !ok {
		return NewError(CodeJobNotFound, "job not found: "+jobID)
	}
	job.PartFilePath = partFilePath
	job.UpdatedAt = time.Now()
	return nil
}

// AppendChunkReport 追加 chunk 上报
// 重复写入以最后一次状态为准，但 completedChunks 不会重复累加同一 chunk
func (s *MemoryJobStore) AppendChunkReport(ctx context.Context, jobID string, report ChunkReport) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	job, ok := s.jobs[jobID]
	if !ok {
		return NewError(CodeJobNotFound, "job not found: "+jobID)
	}

	job.chunkReports[report.ChunkIndex] = report

	if report.Selected {
		if !job.completedChunks[report.ChunkIndex] {
			job.completedChunks[report.ChunkIndex] = true
			job.CompletedChunks++
		}
	}

	job.UpdatedAt = time.Now()
	return nil
}

// AppendQuote 追加 seller 报价（同一 seller 覆盖旧报价，保留最新状态）
func (s *MemoryJobStore) AppendQuote(ctx context.Context, jobID string, quote QuoteReport) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	job, ok := s.jobs[jobID]
	if !ok {
		return NewError(CodeJobNotFound, "job not found: "+jobID)
	}

	job.quotes[quote.SellerPubkey] = quote
	job.UpdatedAt = time.Now()
	return nil
}

// SetOutputPath 设置输出路径
func (s *MemoryJobStore) SetOutputPath(ctx context.Context, jobID string, path string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	job, ok := s.jobs[jobID]
	if !ok {
		return NewError(CodeJobNotFound, "job not found: "+jobID)
	}
	job.OutputFilePath = path
	job.UpdatedAt = time.Now()
	return nil
}

// ListChunks 读取 job 的 chunk 上报快照
func (s *MemoryJobStore) ListChunks(ctx context.Context, jobID string) ([]ChunkReport, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	job, ok := s.jobs[jobID]
	if !ok {
		return nil, false
	}

	reports := make([]ChunkReport, 0, len(job.chunkReports))
	for _, report := range job.chunkReports {
		reports = append(reports, report)
	}
	return reports, true
}

// ListQuotes 读取 job 的 quotes 快照
func (s *MemoryJobStore) ListQuotes(ctx context.Context, jobID string) ([]QuoteReport, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	job, ok := s.jobs[jobID]
	if !ok {
		return nil, false
	}

	quotes := make([]QuoteReport, 0, len(job.quotes))
	for _, quote := range job.quotes {
		quotes = append(quotes, quote)
	}
	return quotes, true
}

// ListJobs 列出所有 job 快照（只读查询）
func (s *MemoryJobStore) ListJobs(ctx context.Context) []Job {
	s.mu.RLock()
	defer s.mu.RUnlock()

	jobs := make([]Job, 0, len(s.jobs))
	for _, job := range s.jobs {
		jobs = append(jobs, jobToSnapshot(job))
	}
	return jobs
}

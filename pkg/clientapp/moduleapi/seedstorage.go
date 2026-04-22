package moduleapi

import "context"

// SeedEnsureInput 是种子系统对单文件构建的输入。
type SeedEnsureInput struct {
	FilePath            string
	RecommendedFileName string
	MimeHint            string
}

// SeedSaveInput 是下载链把已构造好的 seed bytes 交给种子系统时的输入。
type SeedSaveInput struct {
	SeedHash            string
	SeedBytes           []byte
	ChunkHashes         []string
	ChunkCount          uint32
	FileSize            uint64
	RecommendedFileName string
	MimeHint            string
}

// SeedRecord 是种子系统对外的结果。
type SeedRecord struct {
	SeedHash            string
	SeedFilePath        string
	ChunkCount          uint32
	FileSize            uint64
	ChunkHashes         []string
	RecommendedFileName string
	MimeHint            string
	FloorPriceSatPer64K uint64
	ResaleDiscountBPS   uint64
	PricingSource       string
	PriceUpdatedAtUnix  int64
}

// SeedStorage 是种子系统的最小能力面。
type SeedStorage interface {
	EnsureSeedForFile(context.Context, SeedEnsureInput) (SeedRecord, error)
	SaveSeed(context.Context, SeedSaveInput) (SeedRecord, error)
	CleanupOrphanSeeds(context.Context) error
	LoadSeedSnapshot(context.Context, string) (SeedRecord, bool, error)
}

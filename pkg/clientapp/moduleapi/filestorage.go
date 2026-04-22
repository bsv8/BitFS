package moduleapi

// FileStorageHost 是文件存储模块需要的最小主干能力。
//
// 设计说明：
// - 文件存储只关心扫描开关、扫描间隔、启动扫描和种子能力；
// - 不把 Runtime、Config、*clientDB 这样的聚合对象传给模块。
type FileStorageHost interface {
	Store() Store
	ConfigPath() string
	NodePubkeyHex() string
	SeedStorage() SeedStorage
	FSWatchEnabled() bool
	FSRescanIntervalSeconds() uint32
	StartupFullScan() bool
	SellerFloorPriceSatPer64K() uint64
	SellerResaleDiscountBPS() uint64
}

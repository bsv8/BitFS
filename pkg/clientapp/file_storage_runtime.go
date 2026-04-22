package clientapp

import (
	"context"
)

// fileStorageRuntime 是运行态里给下载、直播和文件登记用的最小能力面。
//
// 设计说明：
// - 这里不暴露实现，不传聚合对象；
// - 只保留业务侧真正要调用的动作；
// - 扫描和 watcher 归模块生命周期，不放进运行态主链路。
type fileStorageRuntime interface {
	SelectOutputPath(ctx context.Context, fileName string, fileSize uint64) (string, error)
	SelectLiveSegmentOutputPath(ctx context.Context, streamID string, segmentIndex uint64, fileSize uint64) (string, error)
	RegisterDownloadedFile(ctx context.Context, p registerDownloadedFileParams) (sellerSeed, error)
	EnforceLiveCacheLimit(ctx context.Context, maxBytes uint64) error
}

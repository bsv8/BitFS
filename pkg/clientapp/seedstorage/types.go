package seedstorage

import "github.com/bsv8/BitFS/pkg/clientapp/moduleapi"

// Service 是种子系统的最小能力入口。
//
// 设计说明：
// - 这里只负责 seed 文件、seed 三表和 orphan 清理；
// - 不碰文件存储的目录监听，也不碰 workspace 语义；
// - 上层传进来的 host 只能是窄能力，不拿 Runtime。
type Service struct {
	host moduleapi.Host
}

func NewService(host moduleapi.Host) *Service {
	if host == nil {
		return nil
	}
	return &Service{host: host}
}

type SeedRecord = moduleapi.SeedRecord

type EnsureSeedInput = moduleapi.SeedEnsureInput

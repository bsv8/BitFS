package clientapp

import "github.com/bsv8/BFTP/pkg/obs"

// ServiceName 是 BitFS 客户端进程的统一服务名。
// 设计说明：
// - 业务日志、obs 事件、e2e 断言、pproto domain 统一走同一个名字；
// - 入口层和测试层都不要再手写字符串，避免后续改名散落。
const ServiceName = obs.ServiceBitFSClient

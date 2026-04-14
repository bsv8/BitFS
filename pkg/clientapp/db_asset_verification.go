package clientapp

import (
	"context"
	"database/sql"
	"sync"
	"time"
)

// ==================== Unknown 资产确认流程 ====================
// 设计说明：
// - 对 wallet_utxo 中 allocation_class='unknown' 且 value_satoshi=1 的 UTXO
// - 通过 WOC 查询确认其真实资产类型（BSV20/BSV21/BSV）
// - 确认后更新 allocation_class 并写入相应 fact 表：
//   * BSV UTXO -> fact_bsv_utxos
//   * Token UTXO -> fact_token_lots + fact_token_carrier_links
// - 保持幂等：同一 UTXO 不会重复确认，同一 flow 不会重复写入

const (
	assetVerificationTimeout   = 30 * time.Second
	assetVerificationMaxBatch  = 50
	assetVerificationBaseDelay = 60    // 1 minute
	assetVerificationMaxDelay  = 86400 // 1 day
)

// unknownAssetVerificationTask 控制并发确认任务的执行
type unknownAssetVerificationTask struct {
	mu        sync.Mutex
	inFlight  bool
	lastRunAt int64
}

var assetVerificationTask = &unknownAssetVerificationTask{}

// unknownUTXORow 待确认的 unknown UTXO 行
type unknownUTXORow struct {
	UTXOID        string
	WalletID      string
	Address       string
	TxID          string
	Vout          uint32
	ValueSatoshi  uint64
	CreatedAtUnix int64
}

// enqueueUnknownUTXOToVerification 将 unknown UTXO 加入待确认队列
// 设计说明：
// - 在 reconcileWalletUTXOSet 发现新 UTXO 且 classification 为 unknown 时调用
// - 幂等：已存在的记录不会重复插入

// enqueueUnknownUTXOToVerificationTx 在已有事务内入队 unknown UTXO。
// 设计说明：
// - 事务闭包中禁止再次走 store.Do/store.Tx，避免 actor/事务重入导致超时。

type verificationSQLExec interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

// wocTokenEvidence WOC 返回的 token 证据
type wocTokenEvidence struct {
	IsToken       bool
	TokenStandard string // "bsv20" | "bsv21" | ""
	TokenID       string
	Symbol        string
	QuantityText  string
}

// dbListPendingVerificationItems 从队列表查询待处理项（支持指数退避）

// dbUpdateUTXOAllocationClass 更新 UTXO 的 allocation_class

// runUnknownAssetVerification 执行 unknown 资产确认流程
// 在链同步成功后调用，受并发保护

// 并发保护：同一时刻只执行一个确认任务

// 简单节流：距离上次执行至少 5 秒

// 0. 自动将超过最大重试次数的 pending 项转 failed

// 1. 从队列表获取待处理项

// 2. 批量查询 WOC 获取 token 证据

// WOC 查询失败：指数退避

// 3. 根据证据更新分类并写入 fact

// processAssetVerificationResult 处理单个 UTXO 的确认结果

// 判定新的 allocation_class 和 verification 状态

// 非 token，回落为 BSV

// 更新 wallet_utxo 分类

// 调用统一 fact 入口写入 verified 资产事实
// 设计说明：
// - 通过 ApplyVerifiedAssetFlow 封装 fact 写入细节
// - 幂等写入，同一 UTXO 不会重复写入

// 更新 verification 队列表状态（闭环关键）
// 设计说明：
// - 成功后必须更新 status 为 confirmed_*，否则会被反复重试
// - 记录 woc_response_json 和 last_check_at_unix 用于审计

// 队列状态更新失败不阻塞主流程，仅记录日志

// queryWOCTokenEvidence 查询 WOC 获取指定 outpoint 的 token 证据
// 按优先级：BSV21 > BSV20 > 非 token

// 首先查询 BSV21

// 然后查询 BSV20

// 非 token

// queryBSV21TokenEvidence 查询 BSV21 token 证据

// 复用 wallet_bsv21_woc.go 中的查询

// 通过 scriptHash 匹配 vout

// queryBSV20TokenEvidence 查询 BSV20 token 证据

// 复用 wallet_bsv20_woc.go 中的查询

// 通过 scriptHash 匹配 vout

// updateVerificationQueueSuccess 更新 verification 队列表为成功状态
// 设计说明：
// - 必须调用，否则 pending 项会被无限重试
// - 记录证据快照和检查时间

// updateVerificationBackoff 更新重试时间（指数退避）

package poolcore

// ChainClient 是费用池协议需要的最小链后端能力：
// - client 侧：查询 UTXO / tip height
// - gateway 侧：广播 base/final tx
//
// 生产环境统一使用 chainbridge；E2E 可注入 fake 实现。
type ChainClient interface {
	GetUTXOs(address string) ([]UTXO, error)
	GetTipHeight() (uint32, error)
	Broadcast(txHex string) (string, error)
}

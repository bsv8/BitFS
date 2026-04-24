package poolcore

import (
	"encoding/hex"
	"fmt"

	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	tx "github.com/bsv-blockchain/go-sdk/transaction"
	libs "github.com/bsv8/MultisigPool/pkg/libs"
)

type Actor struct {
	Name    string
	PrivKey *ec.PrivateKey
	PubKey  *ec.PublicKey
	PubHex  string
	Addr    string
}

type Session struct {
	Client      *Actor
	Server      *Actor
	InputAmount uint64
	PoolAmount  uint64
	EndHeight   uint32
	SpendTxFee  uint64

	SpendTxID    string
	BaseTxID     string
	FinalTxID    string
	BaseTxHex    string
	CurrentTxHex string

	Sequence     uint32
	ServerAmount uint64
	ClientAmount uint64
	Status       string

	PaySteps []PayStep
}

// PayStep 记录每一次 Pay 周期的关键数据（用于集成测试核对）。
// 注意：Pay 阶段不会上链（只更新 spend tx 的状态），真正上链的是 Open(base tx) 与 Close(final tx)。
type PayStep struct {
	CycleIndex   uint32 `json:"cycle_index"`
	Sequence     uint32 `json:"sequence"`
	ServerAmount uint64 `json:"server_amount"`
	ClientAmount uint64 `json:"client_amount"`
	UpdatedTxID  string `json:"updated_txid"`
}

type OpenCreateRequest struct {
	ClientPubHex   string
	SpendTxHex     string
	InputAmount    uint64
	SequenceNumber uint32
	ServerAmount   uint64
	ClientSigBytes []byte
}

type OpenCreateResponse struct {
	SpendTxID      string
	ServerSigBytes []byte
}

type OpenBaseTxRequest struct {
	SpendTxID      string
	BaseTxHex      string
	ClientSigBytes []byte
}

type PayConfirmRequest struct {
	SpendTxID      string
	SequenceNumber uint32
	ServerAmount   uint64
	Fee            uint64
	ClientSigBytes []byte
}

type CloseRequest struct {
	SpendTxID      string
	ServerAmount   uint64
	Fee            uint64
	ClientSigBytes []byte
}

type CloseResponse struct {
	FinalTxID      string
	ServerSigBytes []byte
}

// UTXO 是费用池域模型自己的最小未花费输出表示。
// 设计约束：费用池域层不再直接引用具体上游实现类型，避免旧 woc 语义继续渗透。
type UTXO struct {
	TxID  string `json:"txid"`
	Vout  uint32 `json:"vout"`
	Value uint64 `json:"value"`
}

func BuildActor(name string, privHex string, isMain bool) (*Actor, error) {
	priv, err := ec.PrivateKeyFromHex(privHex)
	if err != nil {
		return nil, fmt.Errorf("私钥非法 %s: %w", name, err)
	}
	pub := priv.PubKey()
	addr, err := libs.GetAddressFromPublicKey(pub, isMain)
	if err != nil {
		return nil, fmt.Errorf("派生地址失败 %s: %w", name, err)
	}
	return &Actor{
		Name:    name,
		PrivKey: priv,
		PubKey:  pub,
		PubHex:  hex.EncodeToString(pub.Compressed()),
		Addr:    addr.AddressString,
	}, nil
}

func CalcFee(t *tx.Transaction) uint64 {
	var inTotal uint64
	var outTotal uint64
	for _, in := range t.Inputs {
		if in.SourceTxOutput() != nil {
			inTotal += in.SourceTxOutput().Satoshis
		}
	}
	for _, out := range t.Outputs {
		outTotal += out.Satoshis
	}
	if inTotal <= outTotal {
		return 0
	}
	return inTotal - outTotal
}

func CalcFeeWithInputAmount(t *tx.Transaction, inputAmount uint64) uint64 {
	var outTotal uint64
	for _, out := range t.Outputs {
		outTotal += out.Satoshis
	}
	if inputAmount <= outTotal {
		return 0
	}
	return inputAmount - outTotal
}

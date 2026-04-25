package domainclient

import "fmt"

// SettlementMethod 结算方式枚举。
type SettlementMethod string

const (
	SettlementMethodPool  SettlementMethod = "pool"
	SettlementMethodChain  SettlementMethod = "chain"
)

func (m SettlementMethod) Valid() error {
	switch m {
	case SettlementMethodPool, SettlementMethodChain:
		return nil
	default:
		return fmt.Errorf("invalid settlement_method: %s, must be 'pool' or 'chain'", m)
	}
}

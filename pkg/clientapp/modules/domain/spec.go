package domain

import contractmessage "github.com/bsv8/BFTP-contract/pkg/v1/message"

// 模块固定身份。
const (
	ModuleID            = "domain"
	ModuleVersion       = 1
	CapabilityVersion   = 1
	DomainQuoteVersion  = "bsv8-domain-register-quote-v1"
	ResolveProviderName = "domain_remote"
	ModuleIdentity      = "domain"
	CapabilityNamespace = "domain"
)

// CapabilityItem 返回模块能力描述。
func CapabilityItem() *contractmessage.CapabilityItem {
	return &contractmessage.CapabilityItem{
		ID:      ModuleID,
		Version: CapabilityVersion,
	}
}

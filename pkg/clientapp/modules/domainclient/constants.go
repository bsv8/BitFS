package domainclient

import contractmessage "github.com/bsv8/BFTP-contract/pkg/v1/message"

const (
	ModuleIdentity     = "domainclient"
	ModuleID           = ModuleIdentity
	ModuleVersion      = 1
	CapabilityVersion  = 1
	DomainQuoteVersion = "bsv8-domain-register-quote-v1"
	ResolveProviderName = "domain_remote"
)

// CapabilityItem 返回模块能力描述。
func CapabilityItem() *contractmessage.CapabilityItem {
	return &contractmessage.CapabilityItem{
		ID:      ModuleID,
		Version: CapabilityVersion,
	}
}

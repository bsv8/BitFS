package clientapp

import (
	"fmt"
	"strings"

	ncall "github.com/bsv8/BFTP/pkg/infra/ncall"
)

const defaultPreferredPaymentScheme = ncall.PaymentSchemePool2of2V1

func normalizePreferredPaymentScheme(raw string) (string, error) {
	switch strings.TrimSpace(raw) {
	case "", ncall.PaymentSchemePool2of2V1:
		return ncall.PaymentSchemePool2of2V1, nil
	case ncall.PaymentSchemeChainTxV1:
		return ncall.PaymentSchemeChainTxV1, nil
	default:
		return "", fmt.Errorf("payment scheme not implemented: %s", strings.TrimSpace(raw))
	}
}

func preferredPaymentScheme(rt *Runtime) string {
	if rt == nil {
		return defaultPreferredPaymentScheme
	}
	scheme, err := normalizePreferredPaymentScheme(rt.runIn.Payment.PreferredScheme)
	if err != nil {
		return defaultPreferredPaymentScheme
	}
	return scheme
}

package clientapp

import (
	"fmt"
	"strings"

	contractprotoid "github.com/bsv8/BFTP-contract/pkg/v1/protoid"
)

const defaultPreferredPaymentScheme = contractprotoid.PaymentSchemePool2of2V1

func normalizePreferredPaymentScheme(raw string) (string, error) {
	switch strings.TrimSpace(raw) {
	case "", contractprotoid.PaymentSchemePool2of2V1:
		return contractprotoid.PaymentSchemePool2of2V1, nil
	case contractprotoid.PaymentSchemeChainTxV1:
		return contractprotoid.PaymentSchemeChainTxV1, nil
	default:
		return "", fmt.Errorf("payment scheme not implemented: %s", strings.TrimSpace(raw))
	}
}

func preferredPaymentScheme(rt *Runtime) string {
	if rt == nil {
		return defaultPreferredPaymentScheme
	}
	snapshot := rt.ConfigSnapshot()
	scheme, err := normalizePreferredPaymentScheme(snapshot.Payment.PreferredScheme)
	if err != nil {
		return defaultPreferredPaymentScheme
	}
	return scheme
}

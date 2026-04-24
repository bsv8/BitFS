package moduleapi

import (
	"errors"
	"strings"
)

// Payment error codes.
const (
	CodePaymentSchemeUnavailable  = "PAYMENT_SCHEME_UNAVAILABLE"
	CodePaymentAbilityUnsupported  = "PAYMENT_ABILITY_UNSUPPORTED"
	CodePaymentQuoteExpired        = "PAYMENT_QUOTE_EXPIRED"
	CodePaymentQuoteMismatch       = "PAYMENT_QUOTE_MISMATCH"
	CodePaymentQuoteInvalid        = "PAYMENT_QUOTE_INVALID"
	CodePaymentFailed              = "PAYMENT_FAILED"
	CodePaymentInsufficientBalance = "PAYMENT_INSUFFICIENT_BALANCE"
	CodePaymentBroadcastFailed     = "PAYMENT_BROADCAST_FAILED"
	CodePaymentConfirmFailed       = "PAYMENT_CONFIRM_FAILED"
	CodePaymentRetryExhausted     = "PAYMENT_RETRY_EXHAUSTED"
	CodeSessionNotFound            = "SESSION_NOT_FOUND"
	CodeSessionAlreadyExists       = "SESSION_ALREADY_EXISTS"
	CodeSessionExpired             = "SESSION_EXPIRED"
	CodeSessionSealed              = "SESSION_SEALED"
	CodeRemoteRejected             = "REMOTE_REJECTED"
	CodeRemoteTimeout              = "REMOTE_TIMEOUT"
	CodeRemoteSchemeUnsupported    = "REMOTE_SCHEME_UNSUPPORTED"
	CodeTradeAlreadyExists         = "TRADE_ALREADY_EXISTS"
	CodeTradeNotFound             = "TRADE_NOT_FOUND"
	CodeTradeChunkOutOfRange      = "TRADE_CHUNK_OUT_OF_RANGE"
	CodeTradeInvalidState         = "TRADE_INVALID_STATE"
	CodeTradeArbitrationFailed    = "TRADE_ARBITRATION_FAILED"
	CodeCoverageNotActive         = "COVERAGE_NOT_ACTIVE"
	CodeCoverageRotateRequired    = "COVERAGE_ROTATE_REQUIRED"
	CodePaymentModuleDisabled     = "PAYMENT_MODULE_DISABLED"
	CodePaymentInternalError      = "PAYMENT_INTERNAL_ERROR"
)

// PaymentError is the unified error structure for payment related errors.
type PaymentError struct {
	Code    string
	Message string
}

func (e *PaymentError) Error() string {
	if e == nil {
		return ""
	}
	return strings.TrimSpace(e.Message)
}

func NewPaymentError(code, message string) error {
	return &PaymentError{
		Code:    strings.TrimSpace(code),
		Message: strings.TrimSpace(message),
	}
}

func IsPaymentError(err error) bool {
	var pe *PaymentError
	return errors.As(err, &pe)
}

func AsPaymentError(err error) (*PaymentError, bool) {
	var pe *PaymentError
	if errors.As(err, &pe) {
		return pe, true
	}
	return nil, false
}

func CodeOfPayment(err error) string {
	var pe *PaymentError
	if errors.As(err, &pe) {
		return strings.TrimSpace(pe.Code)
	}
	return ""
}

func MessageOfPayment(err error) string {
	var pe *PaymentError
	if errors.As(err, &pe) {
		return strings.TrimSpace(pe.Message)
	}
	if err == nil {
		return ""
	}
	return strings.TrimSpace(err.Error())
}

func UnavailableScheme(scheme string) error {
	return &PaymentError{
		Code:    CodePaymentSchemeUnavailable,
		Message: "payment scheme not available: " + scheme,
	}
}

func UnsupportedAbility(scheme, ability string) error {
	return &PaymentError{
		Code:    CodePaymentAbilityUnsupported,
		Message: "payment scheme " + scheme + " does not support ability: " + ability,
	}
}

func QuoteExpired() error {
	return &PaymentError{
		Code:    CodePaymentQuoteExpired,
		Message: "payment quote has expired",
	}
}

func QuoteMismatch() error {
	return &PaymentError{
		Code:    CodePaymentQuoteMismatch,
		Message: "payment quote does not match request",
	}
}

func PaymentFailed(reason string) error {
	return &PaymentError{
		Code:    CodePaymentFailed,
		Message: "payment failed: " + reason,
	}
}

func InsufficientBalance(required, available uint64) error {
	return &PaymentError{
		Code:    CodePaymentInsufficientBalance,
		Message: "insufficient balance: required " + formatSatoshi(required) + ", available " + formatSatoshi(available),
	}
}

func SessionNotFound(ref string) error {
	return &PaymentError{
		Code:    CodeSessionNotFound,
		Message: "payment session not found: " + ref,
	}
}

func SessionAlreadyExists(ref string) error {
	return &PaymentError{
		Code:    CodeSessionAlreadyExists,
		Message: "payment session already exists: " + ref,
	}
}

func SessionExpired(ref string) error {
	return &PaymentError{
		Code:    CodeSessionExpired,
		Message: "payment session expired: " + ref,
	}
}

func RemoteRejected(reason string) error {
	return &PaymentError{
		Code:    CodeRemoteRejected,
		Message: "remote rejected: " + reason,
	}
}

func RemoteTimeout() error {
	return &PaymentError{
		Code:    CodeRemoteTimeout,
		Message: "remote timeout",
	}
}

func RemoteSchemeUnsupported(scheme string) error {
	return &PaymentError{
		Code:    CodeRemoteSchemeUnsupported,
		Message: "remote does not support payment scheme: " + scheme,
	}
}

func TradeNotFound(id string) error {
	return &PaymentError{
		Code:    CodeTradeNotFound,
		Message: "trade not found: " + id,
	}
}

func TradeInvalidState(id, state string) error {
	return &PaymentError{
		Code:    CodeTradeInvalidState,
		Message: "trade " + id + " is in invalid state: " + state,
	}
}

func CoverageNotActive(ref string) error {
	return &PaymentError{
		Code:    CodeCoverageNotActive,
		Message: "coverage not active: " + ref,
	}
}

func CoverageRotateRequired(ref string) error {
	return &PaymentError{
		Code:    CodeCoverageRotateRequired,
		Message: "coverage requires rotation: " + ref,
	}
}

func formatSatoshi(n uint64) string {
	return strings.TrimSpace(string(rune(n)))
}

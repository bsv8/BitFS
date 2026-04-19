package domain

import (
	"errors"
	"strings"
)

const (
	CodeBadRequest                     = "BAD_REQUEST"
	CodeRequestCanceled                = "REQUEST_CANCELED"
	CodeDomainResolverUnavailable      = "DOMAIN_RESOLVER_UNAVAILABLE"
	CodeDomainNotResolved              = "DOMAIN_NOT_RESOLVED"
	CodeInvalidPubkey                  = "INVALID_PUBKEY"
	CodeDomainProviderSignatureInvalid = "DOMAIN_PROVIDER_SIGNATURE_INVALID"
	CodeModuleDisabled                 = "MODULE_DISABLED"
)

type Error struct {
	code    string
	message string
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	return strings.TrimSpace(e.message)
}

func NewError(code, message string) error {
	return &Error{code: strings.ToUpper(strings.TrimSpace(code)), message: strings.TrimSpace(message)}
}

func CodeOf(err error) string {
	var typed *Error
	if errors.As(err, &typed) {
		return strings.ToUpper(strings.TrimSpace(typed.code))
	}
	return ""
}

func MessageOf(err error) string {
	var typed *Error
	if errors.As(err, &typed) {
		return strings.TrimSpace(typed.message)
	}
	return strings.TrimSpace(err.Error())
}

func IsHardError(err error) bool {
	switch CodeOf(err) {
	case CodeBadRequest, CodeRequestCanceled, CodeDomainProviderSignatureInvalid:
		return true
	default:
		return false
	}
}

func moduleDisabledErr() error {
	return NewError(CodeModuleDisabled, "module is disabled")
}

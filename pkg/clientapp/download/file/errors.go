package filedownload

import (
	"errors"
	"strings"
)

// 错误码定义
const (
	CodeBadRequest       = "BAD_REQUEST"
	CodeRequestCanceled  = "REQUEST_CANCELED"
	CodeJobNotFound      = "JOB_NOT_FOUND"
	CodeBudgetExceeded   = "BUDGET_EXCEEDED"
	CodeQuoteUnavailable = "QUOTE_UNAVAILABLE"
	CodeQuoteTimeout     = "QUOTE_TIMEOUT"
	CodeDownloadFailed   = "DOWNLOAD_FAILED"
	CodeModuleDisabled   = "MODULE_DISABLED"
)

// Error 业务错误类型
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

// NewError 创建新错误
func NewError(code, message string) error {
	return &Error{code: strings.ToUpper(strings.TrimSpace(code)), message: strings.TrimSpace(message)}
}

// CodeOf 获取错误码
func CodeOf(err error) string {
	if err == nil {
		return ""
	}
	var typed *Error
	if errors.As(err, &typed) {
		return strings.ToUpper(strings.TrimSpace(typed.code))
	}
	return ""
}

// MessageOf 获取错误信息
func MessageOf(err error) string {
	if err == nil {
		return ""
	}
	var typed *Error
	if errors.As(err, &typed) {
		return strings.TrimSpace(typed.message)
	}
	return strings.TrimSpace(err.Error())
}

// IsRetryable 判断错误是否可重试
func IsRetryable(err error) bool {
	switch CodeOf(err) {
	case CodeQuoteUnavailable, CodeQuoteTimeout, CodeDownloadFailed:
		return true
	default:
		return false
	}
}

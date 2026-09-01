package kismockread

// ErrorCode is a closed, safe-to-print failure vocabulary.
type ErrorCode string

const (
	CodeInvalidInput          ErrorCode = "invalid_input"
	CodeConfigurationMissing  ErrorCode = "configuration_missing"
	CodeTokenCacheUnavailable ErrorCode = "token_cache_unavailable"
	CodeTokenMissing          ErrorCode = "token_missing"
	CodeTokenInvalid          ErrorCode = "token_invalid"
	CodeTokenExpired          ErrorCode = "token_expired"
	CodeRequestBlocked        ErrorCode = "request_blocked"
	CodeRedirectBlocked       ErrorCode = "redirect_blocked"
	CodeRequestFailed         ErrorCode = "request_failed"
	CodeResponseInvalid       ErrorCode = "response_invalid"
	CodeBrokerRejected        ErrorCode = "broker_rejected"
)

// SafeError deliberately has no wrapped cause. Callers can print it without
// accidentally exposing a token, credential, Redis URL, or upstream payload.
type SafeError struct {
	Code ErrorCode
}

func (e *SafeError) Error() string {
	if e == nil {
		return ""
	}
	return string(e.Code)
}

func safeError(code ErrorCode) *SafeError {
	return &SafeError{Code: code}
}

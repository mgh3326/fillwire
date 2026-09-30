package kismockedge

// Masked broker-rejection capture. KIS reports rejection detail in its own
// message fields, and a msg1 body can echo account or credential material
// back to the caller. Diagnostics therefore pass through a masker that knows
// every configured secret before any broker field reaches a receipt.

import (
	"bytes"
	"encoding/json"
	"regexp"
	"sort"
	"strings"

	executioncontracts "github.com/mgh3326/broker-edge/execution_contracts"
	"github.com/mgh3326/broker-edge/internal/kismockread"
)

const (
	// redactedValue replaces every secret-shaped span.
	redactedValue = "[redacted]"

	// nonStringFieldMarker replaces an rt_cd/msg_cd/msg1 value whose JSON
	// type is not a string. Serializing objects, arrays, or numbers would
	// carry escaped secret material past the text masker, so non-string
	// fields are dropped to this fixed marker instead.
	nonStringFieldMarker = "<non-string omitted>"

	// maxRejectionCodeLength bounds rt_cd/msg_cd and maxRejectionMsgLength
	// bounds msg1. Values are masked before they are bounded, so a truncated
	// secret can never leak a usable prefix.
	maxRejectionCodeLength = 64
	maxRejectionMsgLength  = 512
)

// accountNumberShape matches a CANO-style account number embedded in free
// text, in dashed form or as a bare run of digits of account length.
var accountNumberShape = regexp.MustCompile(`[0-9]{8}-[0-9]{2}|[0-9]{8,}`)

// credentialShape matches long runs of token or key characters. Legitimate
// KIS message words never reach this length.
var credentialShape = regexp.MustCompile(`[A-Za-z0-9_-]{24,}`)

// secretMasker removes the configured secrets from free text. It is built
// per prepared request because that is the only place where the account,
// application keys, and access token are all already in scope.
type secretMasker struct {
	exact []string
}

func newSecretMasker(config kismockread.Config, token string) *secretMasker {
	candidates := []string{config.AppKey, config.AppSecret, token, config.AccountNo}
	if cano, product, ok := splitAccountNo(config.AccountNo); ok {
		candidates = append(candidates, cano, cano+"-"+product, cano+product)
	}
	var secrets []string
	for _, candidate := range candidates {
		// Only strings long enough to be unambiguous become exact needles; a
		// two-digit product code would otherwise redact ordinary text.
		if candidate = strings.TrimSpace(candidate); len(candidate) >= 6 {
			secrets = append(secrets, candidate)
		}
	}
	sort.Slice(secrets, func(i, j int) bool { return len(secrets[i]) > len(secrets[j]) })
	return &secretMasker{exact: secrets}
}

// maskText removes every secret-shaped span, then bounds the result.
// Masking runs on the full value before bounding so a cut cannot shorten a
// secret into a still-valid prefix.
func (masker *secretMasker) maskText(value string, maxRunes int) string {
	for _, needle := range masker.exact {
		value = strings.ReplaceAll(value, needle, redactedValue)
	}
	value = accountNumberShape.ReplaceAllString(value, redactedValue)
	value = credentialShape.ReplaceAllString(value, redactedValue)
	if runes := []rune(value); len(runes) > maxRunes {
		value = string(runes[:maxRunes])
	}
	return value
}

// maskedField passes a broker field through the masker only when the JSON
// value is a string. Any other JSON type (object, array, number, bool, null)
// is replaced by a fixed marker: a serialized composite could hide secrets
// behind \uXXXX escapes that the text masker cannot see.
func (masker *secretMasker) maskedField(raw json.RawMessage, maxRunes int) string {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return ""
	}
	if trimmed[0] != '"' {
		return nonStringFieldMarker
	}
	var text string
	if json.Unmarshal(trimmed, &text) != nil {
		return nonStringFieldMarker
	}
	return masker.maskText(text, maxRunes)
}

// maskedRejection converts a broker HTTP response into masked diagnostics. It
// returns nil only when the broker never produced an HTTP response.
func maskedRejection(status int, body map[string]json.RawMessage, masker *secretMasker) *executioncontracts.BrokerRejectionV1 {
	if status <= 0 || masker == nil {
		return nil
	}
	return &executioncontracts.BrokerRejectionV1{
		RtCd:       masker.maskedField(body["rt_cd"], maxRejectionCodeLength),
		MsgCd:      masker.maskedField(body["msg_cd"], maxRejectionCodeLength),
		Msg1:       masker.maskedField(body["msg1"], maxRejectionMsgLength),
		HTTPStatus: status,
	}
}

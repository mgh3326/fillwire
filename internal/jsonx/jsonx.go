// Package jsonx holds the strict JSON decoding every contract reader uses:
// unknown fields are refused and exactly one value may be present.
package jsonx

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
)

// ErrTrailingData reports bytes after the one JSON value.
var ErrTrailingData = errors.New("trailing data")

// DecodeStrict decodes exactly one JSON value from data into v and refuses
// unknown object fields. A decode error is returned as encoding/json reports
// it; anything after the value is ErrTrailingData.
func DecodeStrict(data []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return err
	}
	if _, err := dec.Token(); err != io.EOF {
		return ErrTrailingData
	}
	return nil
}

package jsonx

import (
	"errors"
	"testing"
)

func TestDecodeStrict(t *testing.T) {
	type v struct {
		A int `json:"a"`
	}
	var got v
	if err := DecodeStrict([]byte(` {"a":1} `+"\n"), &got); err != nil || got.A != 1 {
		t.Fatalf("got %+v, %v", got, err)
	}
	if err := DecodeStrict([]byte(`{"a":1,"b":2}`), &got); err == nil || errors.Is(err, ErrTrailingData) {
		t.Fatalf("unknown field: %v", err)
	}
	if err := DecodeStrict([]byte(`{"a":1}{"a":2}`), &got); !errors.Is(err, ErrTrailingData) {
		t.Fatalf("second value: %v", err)
	}
	if err := DecodeStrict([]byte(`{"a":1} x`), &got); !errors.Is(err, ErrTrailingData) {
		t.Fatalf("trailing garbage: %v", err)
	}
	if err := DecodeStrict(nil, &got); err == nil {
		t.Fatal("empty input accepted")
	}
}

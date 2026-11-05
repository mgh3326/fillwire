package netpolicy

import (
	"errors"
	"testing"
)

func TestIsLoopbackHost(t *testing.T) {
	for _, tc := range []struct {
		host string
		want bool
	}{
		{"localhost", true},
		{"LOCALHOST", true},
		{"localhost.", true},
		{" localhost ", true},
		{"127.0.0.1", true},
		{"127.8.9.10", true},
		{"::1", true},
		{"", false},
		{"example.com", false},
		{"localhost.example.com", false},
		{"10.0.0.1", false},
	} {
		if got := IsLoopbackHost(tc.host); got != tc.want {
			t.Errorf("IsLoopbackHost(%q) = %v, want %v", tc.host, got, tc.want)
		}
	}
}

func TestValidateRedisURL(t *testing.T) {
	for _, tc := range []struct {
		raw  string
		want error
	}{
		{"rediss://redis.example.invalid:6380/0", nil},
		{"REDISS://redis.example.invalid:6380/0", nil},
		{"redis://localhost:6379/0", nil},
		{"redis://LOCALHOST:6379/0", nil},
		{"redis://127.0.0.1:6379/0", nil},
		{"redis://[::1]:6379/0", nil},
		{"unix:///var/run/redis.sock", nil},
		{"redis://redis.example.invalid:6379/0", ErrRedisURLPlaintext},
		{"rediss:///0", ErrRedisURLPlaintext},
		{"unix://", ErrRedisURLPlaintext},
		{"http://localhost:6379", ErrRedisURLPlaintext},
		{"", ErrRedisURLInvalid},
		{"localhost:6379", ErrRedisURLPlaintext},
		{"not a url", ErrRedisURLInvalid},
	} {
		if err := ValidateRedisURL(tc.raw); !errors.Is(err, tc.want) {
			t.Errorf("ValidateRedisURL(%q) = %v, want %v", tc.raw, err, tc.want)
		}
	}
}

package gatewayd

import (
	"testing"
	"time"

	"github.com/mgh3326/broker-edge/internal/kismockread"
)

func TestConfigPinsVTSAndLoopbackListener(t *testing.T) {
	values := map[string]string{
		"KIS_MOCK_APP_KEY":    "app-key-for-test",
		"KIS_MOCK_APP_SECRET": "app-secret-for-test",
		"REDIS_URL":           "redis://127.0.0.1:6379/0",
	}
	config, err := ConfigFromEnv(func(name string) string { return values[name] })
	if err != nil {
		t.Fatal(err)
	}
	if config.BaseURL != kismockread.MockBaseURL || config.ListenAddress != "127.0.0.1:8791" {
		t.Fatalf("config = %#v", config)
	}
	if !loopbackAddress("127.0.0.1:0") || !loopbackAddress("[::1]:8791") {
		t.Fatal("loopback listener rejected")
	}
	values["GATEWAYD_LISTEN_ADDR"] = "0.0.0.0:8791"
	if _, err := ConfigFromEnv(func(name string) string { return values[name] }); err == nil {
		t.Fatal("public listener was accepted")
	}
	config = testGatewayConfig()
	config.BaseURL = "https://example.test"
	if _, err := NewEnsureService(newFakeRedisStore(testGatewayNow), &fakeOAuthIssuer{}, config); err == nil {
		t.Fatal("non-VTS token namespace was accepted")
	}
}

func TestParseEnsureInterval(t *testing.T) {
	interval, err := ParseEnsureInterval([]string{"--ensure-interval", "5m"})
	if err != nil || interval != 5*time.Minute {
		t.Fatalf("interval = %v, %v", interval, err)
	}
	if _, err := ParseEnsureInterval([]string{"--ensure-interval", "0s"}); err == nil {
		t.Fatal("zero interval accepted")
	}
}

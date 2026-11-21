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

func TestConfigOnlyLoadsExplicitProviders(t *testing.T) {
	values := map[string]string{
		"REDIS_URL":              "redis://127.0.0.1:6379/0",
		"KIS_APP_KEY":            "live-app-key-for-test",
		"KIS_APP_SECRET":         "live-app-secret-for-test",
		"TOSS_API_CLIENT_ID":     "toss-client-id-for-test",
		"TOSS_API_CLIENT_SECRET": "toss-client-secret-for-test",
	}
	lookup := func(name string) string { return values[name] }
	if _, err := ConfigFromEnv(lookup); err == nil {
		t.Fatal("default kis-mock configuration accepted absent mock credentials")
	}
	config, err := ConfigFromEnvForProviders(lookup, []TokenProvider{ProviderKISLive, ProviderToss})
	if err != nil {
		t.Fatal(err)
	}
	if len(config.ProviderConfigs) != 2 {
		t.Fatalf("configured providers = %d, want 2", len(config.ProviderConfigs))
	}
	live, found := config.providerConfig(ProviderKISLive)
	if !found || live.BaseURL != KISLiveBaseURL {
		t.Fatal("KIS live was not configured with its pinned base URL")
	}
	toss, found := config.providerConfig(ProviderToss)
	if !found || toss.BaseURL != TossBaseURL {
		t.Fatal("Toss was not configured with its pinned base URL")
	}
	if _, found := config.providerConfig(ProviderKISMock); found {
		t.Fatal("unselected kis-mock provider was configured")
	}
}

func TestParseRunOptionsUsesClosedProviderList(t *testing.T) {
	options, err := ParseRunOptions([]string{"--ensure-interval", "5m", "--providers", "kis-mock,toss"})
	if err != nil || options.EnsureInterval != 5*time.Minute {
		t.Fatalf("options = %#v, %v", options, err)
	}
	if len(options.Providers) != 2 || options.Providers[0] != ProviderKISMock || options.Providers[1] != ProviderToss {
		t.Fatalf("providers = %#v", options.Providers)
	}
	defaultOptions, err := ParseRunOptions(nil)
	if err != nil || len(defaultOptions.Providers) != 1 || defaultOptions.Providers[0] != ProviderKISMock {
		t.Fatalf("default options = %#v, %v", defaultOptions, err)
	}
	for _, args := range [][]string{
		{"--providers", "kis-live,unknown"},
		{"--providers", "kis-mock,kis-mock"},
		{"--providers", ""},
	} {
		if _, err := ParseRunOptions(args); err == nil {
			t.Fatalf("invalid provider list accepted: %#v", args)
		}
	}
}

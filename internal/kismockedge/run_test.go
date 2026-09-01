package kismockedge

import "testing"

func TestServerDefaultsToLoopbackAndRejectsPublicBind(t *testing.T) {
	config, err := ServerConfigFromEnv(func(string) string { return "" })
	if err != nil {
		t.Fatal(err)
	}
	if config.ListenAddress != "127.0.0.1:8080" {
		t.Fatalf("default listener = %q", config.ListenAddress)
	}
	if !loopbackAddress("127.0.0.1:0") || !loopbackAddress("[::1]:8080") {
		t.Fatal("loopback addresses rejected")
	}
	if loopbackAddress("0.0.0.0:8080") || loopbackAddress("192.0.2.1:8080") {
		t.Fatal("non-loopback address accepted")
	}
	_, err = ServerConfigFromEnv(func(name string) string {
		if name == "BROKER_EDGE_LISTEN_ADDR" {
			return "0.0.0.0:8080"
		}
		return ""
	})
	if err == nil {
		t.Fatal("public listener override was accepted")
	}
}

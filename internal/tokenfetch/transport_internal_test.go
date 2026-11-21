package tokenfetch

import (
	"net/http"
	"testing"
)

// Go's default transport exempts loopback from proxies, so a loopback server
// cannot show that ambient proxy variables are ignored; check the transport.
func TestClientTransportIgnoresProxyEnvironment(t *testing.T) {
	t.Setenv("HTTPS_PROXY", "http://proxy.example.invalid:3128")
	client, err := NewClient("https://gatewayd.example.invalid:8791", "credential")
	if err != nil {
		t.Fatal(err)
	}
	transport := client.http.Transport.(*http.Transport)
	req, _ := http.NewRequest(http.MethodPost, "https://gatewayd.example.invalid:8791/x", nil)
	if transport.Proxy != nil {
		if proxy, _ := transport.Proxy(req); proxy != nil {
			t.Fatalf("proxy = %v", proxy)
		}
	}
	if client.http.Timeout != RequestTimeout || client.http.CheckRedirect == nil {
		t.Fatal("timeout or redirect policy missing")
	}
}

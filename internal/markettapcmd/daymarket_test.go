package markettapcmd

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/mgh3326/fillwire/internal/kisread"
	"github.com/mgh3326/fillwire/internal/kst"
)

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

// pollOnce runs the command against a fake KIS with an open day-market window
// and returns every request it received plus the JSONL rows.
func pollOnce(t *testing.T, extra ...string) ([]string, string) {
	t.Helper()
	label := time.Now().Add(-5 * time.Minute).In(kst.Zone)
	var mu sync.Mutex
	var requests []string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		mu.Lock()
		requests = append(requests, fmt.Sprintf("%s %s %s %s", request.Method, request.URL.Path,
			request.Header.Get("tr_id"), request.URL.Query().Get("EXCD")))
		mu.Unlock()
		if !strings.HasPrefix(request.URL.Query().Get("EXCD"), "BA") {
			writer.Write([]byte(`{"rt_cd":"0","output2":[]}`))
			return
		}
		fmt.Fprintf(writer, `{"rt_cd":"0","output2":[{"kymd":%q,"khms":%q,"open":"1","high":"1","low":"1","last":"1","evol":"1","eamt":"1"}]}`,
			label.Format("20060102"), label.Format("150405"))
	}))
	t.Cleanup(server.Close)

	mini := miniredis.RunT(t)
	if err := mini.Set(kisread.AccessTokenLiveKey,
		fmt.Sprintf(`{"access_token":"fixture-token","expires_at":%d}`, time.Now().Add(time.Hour).Unix())); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	targets := filepath.Join(dir, "targets.json")
	calendar := filepath.Join(dir, "calendar.json")
	output := filepath.Join(dir, "rows.jsonl")
	writeFile(t, targets, `{"targets":[{"market":"us","symbol":"NVDA","venue":"NASD"}]}`)
	writeFile(t, calendar, fmt.Sprintf(`{"us_day_market":[{"start":%q,"end":%q}]}`,
		time.Now().Add(-time.Hour).Format(time.RFC3339), time.Now().Add(time.Hour).Format(time.RFC3339)))
	t.Setenv("FILLWIRE_TEST_APP_KEY", "fixture-app-key")
	t.Setenv("FILLWIRE_TEST_APP_SECRET", "fixture-app-secret")

	var stdout, stderr bytes.Buffer
	code := Run(append([]string{
		"-enabled", "-once", "-targets", targets, "-calendar", calendar, "-out", output,
		"-host", server.URL, "-token-redis", "redis://" + mini.Addr(),
		"-app-key-env", "FILLWIRE_TEST_APP_KEY", "-app-secret-env", "FILLWIRE_TEST_APP_SECRET",
	}, extra...), &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit = %d, stderr %q", code, stderr.String())
	}
	rows, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	return slices.Clone(requests), string(rows)
}

const minuteRead = "GET /uapi/overseas-price/v1/quotations/inquire-time-itemchartprice HHDFS76950200 "

func TestUSDayMarketIsOffByDefault(t *testing.T) {
	requests, rows := pollOnce(t)
	if !slices.Equal(requests, []string{minuteRead + "NAS"}) {
		t.Fatalf("requests = %q, want the regular read only", requests)
	}
	if rows != "" {
		t.Fatalf("rows = %q", rows)
	}
}

// The flag adds one request per US target and that request is a quotation GET:
// no order method, path or transaction is sent.
func TestUSDayMarketFlagAddsOnlyTheQuotationRead(t *testing.T) {
	requests, rows := pollOnce(t, "-us-day-market")
	if !slices.Equal(requests, []string{minuteRead + "NAS", minuteRead + "BAQ"}) {
		t.Fatalf("requests = %q", requests)
	}
	if strings.Count(rows, "\n") != 1 || !strings.Contains(rows, `"venue":"BAQ","session_segment":"US_DAY","source":"KIS"`) {
		t.Fatalf("rows = %q", rows)
	}
}

func TestUSDayMarketFlagNeedsDayWindows(t *testing.T) {
	dir := t.TempDir()
	targets := filepath.Join(dir, "targets.json")
	calendar := filepath.Join(dir, "calendar.json")
	output := filepath.Join(dir, "rows.jsonl")
	writeFile(t, targets, `{"targets":[{"market":"us","symbol":"NVDA","venue":"NASD"}]}`)
	writeFile(t, calendar, `{}`)
	var stdout, stderr bytes.Buffer
	code := Run([]string{"-enabled", "-us-day-market", "-targets", targets, "-calendar", calendar, "-out", output, "-once"}, &stdout, &stderr)
	if code != ExitConfiguration || !strings.Contains(stderr.String(), "us_day_market") {
		t.Fatalf("exit = %d, stderr %q", code, stderr.String())
	}
	if _, err := os.Stat(output); !os.IsNotExist(err) {
		t.Fatal("a refused configuration created the output file")
	}
}

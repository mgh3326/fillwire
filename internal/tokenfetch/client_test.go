package tokenfetch_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/fillwire/internal/tokenfetch"
	"github.com/mgh3326/fillwire/internal/tokenfetch/fetchtest"
)

const credential = "consumer-credential-xyz"

func newPair(t *testing.T) (*fetchtest.Server, *tokenfetch.Client) {
	t.Helper()
	server := fetchtest.New(&fetchtest.Clock{}, credential)
	t.Cleanup(server.Close)
	client, err := tokenfetch.NewClient(server.URL, credential)
	if err != nil {
		t.Fatal(err)
	}
	return server, client
}

func TestFetchRequestShapeAndAnswer(t *testing.T) {
	server, client := newPair(t)
	token, err := client.Fetch(context.Background(), "kis-live-approval", "")
	if err != nil {
		t.Fatal(err)
	}
	if token.Value() != "fake-kis-live-approval-token-1" || token.Source != tokenfetch.SourceIssued || token.Remaining(time.Now()) < 23*time.Hour {
		t.Fatalf("token = %+v", token)
	}
	failed := tokenfetch.Hash(token.Value())
	if _, err := client.Fetch(context.Background(), "kis-live-approval", failed); err != nil {
		t.Fatal(err)
	}
	calls := server.Calls()
	if len(calls) != 2 || calls[0].Auth != "Bearer "+credential || calls[0].Body != "{}" || calls[0].Failed != "" ||
		calls[1].Failed != failed || calls[1].Body != `{"failed_token_sha256":"`+failed+`"}` {
		t.Fatalf("calls = %+v", calls)
	}
}

func TestCredentialSentWithExactlyOneBearer(t *testing.T) {
	for _, given := range []string{credential, "Bearer " + credential, "  Bearer " + credential + " "} {
		server := fetchtest.New(&fetchtest.Clock{}, credential)
		client, err := tokenfetch.NewClient(server.URL, given)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := client.Fetch(context.Background(), "toss", ""); err != nil {
			t.Fatalf("credential %q: %v", given, err)
		}
		if calls := server.Calls(); len(calls) != 1 || calls[0].Auth != "Bearer "+credential {
			t.Fatalf("credential %q sent %+v", given, calls)
		}
		server.Close()
	}
	if _, err := tokenfetch.NewClient("http://127.0.0.1:1", "Bearer "); err == nil {
		t.Fatal("empty credential after the prefix accepted")
	}
}

func TestFetchStatusesAreTypedAndCarryNoBody(t *testing.T) {
	server, client := newPair(t)
	bad, _ := tokenfetch.NewClient(server.URL, "wrong-credential")
	server.Forbid("kis-mock")
	leak := `{"token":"leaked-` + credential + `"}`
	for _, tc := range []struct {
		name     string
		do       func() error
		want     error
		failWith int
	}{
		{"401", func() error { _, err := bad.Fetch(context.Background(), "toss", ""); return err }, tokenfetch.ErrUnauthenticated, 0},
		{"403", func() error { _, err := client.Fetch(context.Background(), "kis-mock", ""); return err }, tokenfetch.ErrProviderNotAllowed, 0},
		{"404", func() error { _, err := client.Fetch(context.Background(), "nope", ""); return err }, tokenfetch.ErrUnknownProvider, 0},
		{"400 server", func() error { _, err := client.Fetch(context.Background(), "toss", ""); return err }, tokenfetch.ErrBadRequest, 400},
		{"400 local bad hash", func() error { _, err := client.Fetch(context.Background(), "toss", "ABC"); return err }, tokenfetch.ErrBadRequest, 0},
		{"503", func() error { _, err := client.Fetch(context.Background(), "toss", ""); return err }, tokenfetch.ErrIssueUnavailable, 503},
		{"500", func() error { _, err := client.Fetch(context.Background(), "toss", ""); return err }, tokenfetch.ErrUnavailable, 500},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server.FailWith(tc.failWith, leak)
			err := tc.do()
			if !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
			if strings.Contains(err.Error(), "leaked") || strings.Contains(err.Error(), credential) || strings.Contains(err.Error(), server.URL) {
				t.Fatalf("error leaks: %q", err)
			}
		})
	}
}

// A status the client already knows must survive a body that cannot be read.
func TestFetchKeepsTheStatusWhenTheBodyReadFails(t *testing.T) {
	for status, want := range map[int]error{
		http.StatusUnauthorized:       tokenfetch.ErrUnauthenticated,
		http.StatusForbidden:          tokenfetch.ErrProviderNotAllowed,
		http.StatusNotFound:           tokenfetch.ErrUnknownProvider,
		http.StatusBadRequest:         tokenfetch.ErrBadRequest,
		http.StatusServiceUnavailable: tokenfetch.ErrIssueUnavailable,
	} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Length", "100")
			w.WriteHeader(status)
			_, _ = w.Write([]byte(`{"err`)) // then the connection ends early
		}))
		client, _ := tokenfetch.NewClient(server.URL, credential)
		if _, err := client.Fetch(context.Background(), "toss", ""); !errors.Is(err, want) {
			t.Errorf("status %d with a cut-off body: err = %v, want %v", status, err, want)
		}
		server.Close()
	}
}

func TestFetchRejectsMalformedAnswers(t *testing.T) {
	future := time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
	good := func() map[string]string {
		return map[string]string{"provider": "toss", "token": "tok-1", "expires_at": future, "token_sha256": tokenfetch.Hash("tok-1"), "source": "issued"}
	}
	for name, mutate := range map[string]func(map[string]string){
		"wrong provider": func(m map[string]string) { m["provider"] = "kis-live" },
		"empty token":    func(m map[string]string) { m["token"] = "" },
		"wrong hash":     func(m map[string]string) { m["token_sha256"] = tokenfetch.Hash("other") },
		"expired":        func(m map[string]string) { m["expires_at"] = time.Now().Add(-time.Minute).UTC().Format(time.RFC3339) },
		"bad expiry":     func(m map[string]string) { m["expires_at"] = "tomorrow" },
		"bad source":     func(m map[string]string) { m["source"] = "cache" },
	} {
		t.Run(name, func(t *testing.T) {
			answer := good()
			mutate(answer)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				fmt.Fprintf(w, `{"provider":%q,"token":%q,"expires_at":%q,"token_sha256":%q,"source":%q}`,
					answer["provider"], answer["token"], answer["expires_at"], answer["token_sha256"], answer["source"])
			}))
			defer server.Close()
			client, _ := tokenfetch.NewClient(server.URL, credential)
			_, err := client.Fetch(context.Background(), "toss", "")
			if !errors.Is(err, tokenfetch.ErrUnavailable) || strings.Contains(err.Error(), "tok-1") {
				t.Fatalf("err = %v", err)
			}
		})
	}
	t.Run("not json", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, "<html>tok-1") }))
		defer server.Close()
		client, _ := tokenfetch.NewClient(server.URL, credential)
		if _, err := client.Fetch(context.Background(), "toss", ""); !errors.Is(err, tokenfetch.ErrUnavailable) {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("same token as the failed one", func(t *testing.T) {
		answer := good()
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			fmt.Fprintf(w, `{"provider":"toss","token":"tok-1","expires_at":%q,"token_sha256":%q,"source":"memory"}`, answer["expires_at"], answer["token_sha256"])
		}))
		defer server.Close()
		client, _ := tokenfetch.NewClient(server.URL, credential)
		if _, err := client.Fetch(context.Background(), "toss", tokenfetch.Hash("tok-1")); !errors.Is(err, tokenfetch.ErrRejectedTokenReturn) {
			t.Fatalf("err = %v", err)
		}
	})
}

func TestClientTransportRules(t *testing.T) {
	for _, tc := range []struct {
		url, credential string
		ok              bool
	}{
		{"https://gatewayd.example.invalid:8791", credential, true},
		{"http://127.0.0.1:8791", credential, true},
		{"http://localhost:8791", credential, true},
		{"http://gatewayd.example.invalid:8791", credential, false},
		{"https://gatewayd.example.invalid:8791", "", false},
		{"ftp://127.0.0.1", credential, false},
		{"http://127.0.0.1:8791?x=1", credential, false},
		{"http://user:pw@127.0.0.1:8791", credential, false},
	} {
		_, err := tokenfetch.NewClient(tc.url, tc.credential)
		if (err == nil) != tc.ok {
			t.Errorf("NewClient(%q, %q) err = %v, want ok=%t", tc.url, tc.credential, err, tc.ok)
		}
	}
}

func TestClientFollowsNoRedirects(t *testing.T) {
	var followed bool
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { followed = true }))
	defer target.Close()
	redirector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL+"/x", http.StatusTemporaryRedirect)
	}))
	defer redirector.Close()
	client, _ := tokenfetch.NewClient(redirector.URL, credential)
	if _, err := client.Fetch(context.Background(), "toss", ""); !errors.Is(err, tokenfetch.ErrUnavailable) || followed {
		t.Fatalf("err = %v followed = %t", err, followed)
	}
}

func TestTokenNeverFormatsItsValue(t *testing.T) {
	token := tokenfetch.NewToken("super-secret-value", time.Now(), tokenfetch.SourceMemory)
	for _, rendered := range []string{fmt.Sprintf("%v", token), fmt.Sprintf("%+v", token), fmt.Sprintf("%#v", token), fmt.Sprintf("%s", token), fmt.Sprint(&token)} {
		if strings.Contains(rendered, "super-secret-value") {
			t.Fatalf("rendered %q", rendered)
		}
	}
}

package recording

import (
	"errors"
	"testing"
	"time"
)

// stalledRecord holds its append inside the encoder, as a blocked disk write would.
type stalledRecord struct {
	started chan struct{}
	release <-chan struct{}
}

func (r stalledRecord) MarshalJSON() ([]byte, error) {
	close(r.started)
	<-r.release
	return []byte(`{}`), nil
}

func TestSinkCloseWithinBoundsStalledAppend(t *testing.T) {
	sink, err := NewSink(t.TempDir(), "quotes")
	if err != nil {
		t.Fatal(err)
	}
	started, release := make(chan struct{}), make(chan struct{})
	appended := make(chan error, 1)
	go func() {
		appended <- sink.Append("quotes", "005930", time.Now().UTC(), stalledRecord{started: started, release: release})
	}()
	<-started
	if err := sink.CloseWithin(10 * time.Millisecond); !errors.Is(err, ErrCloseTimeout) {
		t.Fatalf("CloseWithin with a stalled append = %v, want ErrCloseTimeout", err)
	}
	if err := sink.Append("quotes", "005930", time.Now().UTC(), 1); err == nil {
		t.Fatal("append accepted after close began")
	}
	close(release)
	if err := <-appended; err != nil {
		t.Fatalf("stalled append after release: %v", err)
	}
	if err := sink.CloseWithin(time.Second); err != nil {
		t.Fatalf("CloseWithin after the append finished: %v", err)
	}
}

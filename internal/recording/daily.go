// Package recording writes append-only market evidence into UTC day partitions.
package recording

import (
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"time"
)

var safeName = regexp.MustCompile(`^[A-Za-z0-9_-]{1,32}$`)

// DailyWriter serializes records into <root>/<stream>/<UTC day>/<name>.jsonl.
// Closed days are synced and gzip-compressed; existing files are appended.
type DailyWriter struct {
	mu     sync.Mutex
	root   string
	stream string
	name   string
	day    string
	file   *os.File
	closed bool
}

func NewDailyWriter(root, stream, name string) (*DailyWriter, error) {
	if root == "" || !safeName.MatchString(stream) || !safeName.MatchString(name) {
		return nil, errors.New("recording: 루트, 스트림 또는 이름이 잘못되었습니다")
	}
	return &DailyWriter{root: root, stream: stream, name: name}, nil
}

func (w *DailyWriter) Append(at time.Time, value any) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return errors.New("recording: 기록기가 닫혔습니다")
	}
	day := at.UTC().Format("2006-01-02")
	if day != w.day {
		if err := w.rotate(day); err != nil {
			return err
		}
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("recording: 인코딩 실패: %w", err)
	}
	if _, err := w.file.Write(append(encoded, '\n')); err != nil {
		return fmt.Errorf("recording: 추가 기록 실패: %w", err)
	}
	return nil
}

func (w *DailyWriter) rotate(day string) error {
	if w.file != nil {
		if err := w.closeDay(); err != nil {
			return err
		}
	}
	dir := filepath.Join(w.root, w.stream, day)
	if err := os.MkdirAll(dir, 0750); err != nil {
		return fmt.Errorf("recording: 날짜 디렉터리 생성 실패: %w", err)
	}
	path := filepath.Join(dir, w.name+".jsonl")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0640)
	if err != nil {
		return fmt.Errorf("recording: 파일 열기 실패: %w", err)
	}
	w.file, w.day = file, day
	return nil
}

func (w *DailyWriter) closeDay() error {
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("recording: sync: %w", err)
	}
	if err := w.file.Close(); err != nil {
		return fmt.Errorf("recording: close: %w", err)
	}
	old := w.file
	w.file = nil
	if err := gzipFile(old.Name()); err != nil {
		return err
	}
	return nil
}

func gzipFile(path string) error {
	in, err := os.Open(path)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(path+".gz", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0640)
	if err != nil {
		return fmt.Errorf("recording: gzip 파일 생성 실패: %w", err)
	}
	gz := gzip.NewWriter(out)
	_, copyErr := io.Copy(gz, in)
	closeGZErr := gz.Close()
	closeErr := out.Close()
	if copyErr != nil || closeGZErr != nil || closeErr != nil {
		_ = os.Remove(path + ".gz")
		return errors.Join(copyErr, closeGZErr, closeErr)
	}
	if err := os.Remove(path); err != nil {
		return err
	}
	return nil
}

func (w *DailyWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	w.closed = true
	if w.file == nil {
		return nil
	}
	if err := w.file.Sync(); err != nil {
		return err
	}
	if err := w.file.Close(); err != nil {
		return err
	}
	w.file = nil
	return nil
}

// Sink manages independent per-stream and per-name day writers.
type Sink struct {
	mu      sync.Mutex
	root    string
	writers map[string]*DailyWriter
	closed  bool
}

func NewSink(root string) (*Sink, error) {
	if root == "" {
		return nil, errors.New("recording: 루트 디렉터리가 필요합니다")
	}
	return &Sink{root: root, writers: map[string]*DailyWriter{}}, nil
}
func (s *Sink) Append(stream, name string, at time.Time, value any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errors.New("recording: 싱크가 닫혔습니다")
	}
	key := stream + "/" + name
	w := s.writers[key]
	if w == nil {
		var err error
		w, err = NewDailyWriter(s.root, stream, name)
		if err != nil {
			return err
		}
		s.writers[key] = w
	}
	return w.Append(at, value)
}
func (s *Sink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	var errs []error
	for _, w := range s.writers {
		if err := w.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

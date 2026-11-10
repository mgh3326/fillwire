// Package recording writes append-only market evidence into UTC day partitions.
package recording

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"syscall"
	"time"
)

var (
	safeName     = regexp.MustCompile(`^[A-Za-z0-9_-]{1,32}$`)
	safeFileName = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,32}$`)
)

// SafeName reports whether name is safe as one path component.
func SafeName(name string) bool {
	return name != "." && name != ".." && safeFileName.MatchString(name)
}

// StreamLock excludes another process from writing the same stream under a root.
type StreamLock struct{ file *os.File }

func AcquireStreamLock(root, stream string) (*StreamLock, error) {
	if root == "" || !safeName.MatchString(stream) {
		return nil, errors.New("recording: 잘못된 루트 또는 스트림 이름")
	}
	if err := os.MkdirAll(root, 0750); err != nil {
		return nil, fmt.Errorf("recording: 루트 디렉터리 생성 실패: %w", err)
	}
	file, err := os.OpenFile(filepath.Join(root, ".fillwire-"+stream+".lock"), os.O_CREATE|os.O_RDWR, 0640)
	if err != nil {
		return nil, fmt.Errorf("recording: 스트림 잠금 파일 열기 실패: %w", err)
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("recording: %s 스트림이 이미 다른 프로세스에서 기록 중입니다", stream)
	}
	return &StreamLock{file: file}, nil
}

func (l *StreamLock) Close() error {
	if l == nil || l.file == nil {
		return nil
	}
	err := syscall.Flock(int(l.file.Fd()), syscall.LOCK_UN)
	closeErr := l.file.Close()
	l.file = nil
	return errors.Join(err, closeErr)
}

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
	if root == "" || !safeName.MatchString(stream) || !SafeName(name) {
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
	if w.day != "" && day < w.day {
		// A clock correction must not reopen an older partition while a newer
		// one is active. Preserve the original timestamp in the record.
		day = w.day
	}
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
	if err := restoreCompressed(path); err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0640)
	if err != nil {
		return fmt.Errorf("recording: 파일 열기 실패: %w", err)
	}
	w.file, w.day = file, day
	return nil
}

// restoreCompressed makes a previously closed partition appendable after a
// clock rollback or a restart configured against an older date.
func restoreCompressed(path string) error {
	compressed := path + ".gz"
	if _, err := os.Stat(compressed); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".fillwire-restore-*.tmp")
	if err != nil {
		return err
	}
	if err := tmp.Chmod(0640); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
		return err
	}
	cleanup := func() { _ = tmp.Close(); _ = os.Remove(tmp.Name()) }
	f, err := os.Open(compressed)
	if err != nil {
		cleanup()
		return err
	}
	gz, err := gzip.NewReader(f)
	if err != nil {
		_ = f.Close()
		cleanup()
		return err
	}
	existingHash := sha256.New()
	_, copyErr := io.Copy(io.MultiWriter(tmp, existingHash), gz)
	closeGZErr := gz.Close()
	closeInErr := f.Close()
	if copyErr != nil || closeGZErr != nil || closeInErr != nil {
		cleanup()
		return errors.Join(copyErr, closeGZErr, closeInErr)
	}
	if raw, err := os.Open(path); err == nil {
		rawHash := sha256.New()
		_, copyErr = io.Copy(rawHash, raw)
		if copyErr == nil && !bytes.Equal(existingHash.Sum(nil), rawHash.Sum(nil)) {
			_, copyErr = raw.Seek(0, io.SeekStart)
			if copyErr == nil {
				_, copyErr = io.Copy(tmp, raw)
			}
		}
		closeInErr = raw.Close()
		if copyErr != nil || closeInErr != nil {
			cleanup()
			return errors.Join(copyErr, closeInErr)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		cleanup()
		return err
	}
	if err := tmp.Sync(); err != nil {
		cleanup()
		return err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmp.Name())
		return err
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		_ = os.Remove(tmp.Name())
		return err
	}
	if err := os.Remove(compressed); err != nil {
		return err
	}
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
	out, err := os.CreateTemp(filepath.Dir(path), ".fillwire-gzip-*.tmp")
	if err != nil {
		return fmt.Errorf("recording: gzip 파일 생성 실패: %w", err)
	}
	if err := out.Chmod(0640); err != nil {
		_ = out.Close()
		_ = os.Remove(out.Name())
		return err
	}
	gz := gzip.NewWriter(out)
	_, copyErr := io.Copy(gz, in)
	closeGZErr := gz.Close()
	syncErr := out.Sync()
	closeErr := out.Close()
	if copyErr != nil || closeGZErr != nil || syncErr != nil || closeErr != nil {
		_ = os.Remove(out.Name())
		return errors.Join(copyErr, closeGZErr, syncErr, closeErr)
	}
	if err := os.Rename(out.Name(), path+".gz"); err != nil {
		_ = os.Remove(out.Name())
		return fmt.Errorf("recording: gzip 파일 교체 실패: %w", err)
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
	locks   map[string]*StreamLock
	closed  bool
}

func NewSink(root string, streams ...string) (*Sink, error) {
	if root == "" {
		return nil, errors.New("recording: 루트 디렉터리가 필요합니다")
	}
	s := &Sink{root: root, writers: map[string]*DailyWriter{}, locks: map[string]*StreamLock{}}
	for _, stream := range streams {
		if err := s.lockStream(stream); err != nil {
			_ = s.Close()
			return nil, err
		}
	}
	return s, nil
}

func (s *Sink) lockStream(stream string) error {
	if s.locks[stream] != nil {
		return nil
	}
	lock, err := AcquireStreamLock(s.root, stream)
	if err != nil {
		return err
	}
	s.locks[stream] = lock
	return nil
}

func (s *Sink) Append(stream, name string, at time.Time, value any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errors.New("recording: 싱크가 닫혔습니다")
	}
	if err := s.lockStream(stream); err != nil {
		return err
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
	for _, lock := range s.locks {
		if err := lock.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

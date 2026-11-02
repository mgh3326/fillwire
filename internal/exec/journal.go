package exec

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
)

// JournalVersion is the journal line format.
const JournalVersion = 1

// ErrCorrupt marks a journal that does not replay: a bad line before the
// last, a sequence gap, a record Apply refuses, or a state digest that does
// not match the one written. The executor must not act on such a journal.
var ErrCorrupt = errors.New("exec: corrupt journal")

// ErrJournalLocked marks a journal another open handle holds. Two writers
// would append at separate offsets and overwrite each other's records.
var ErrJournalLocked = errors.New("exec: journal is open elsewhere")

type journalLine struct {
	V     int    `json:"v"`
	Seq   uint64 `json:"seq"`
	Rec   Record `json:"rec"`
	State string `json:"state"` // Digest of the state after Rec
}

// Journal is the executor's append-only write-ahead log. A record is synced
// to disk before the executor acts on it. One handle holds the journal at a
// time: OpenJournal takes an exclusive lock that Close releases.
type Journal struct {
	f   *os.File
	seq uint64
}

// OpenJournal opens or creates the journal at path, locks it, and replays it.
// It fails with ErrJournalLocked while another handle holds the journal. A
// torn last line (a crash during its write; nothing acted on it) is cut off
// so the next record starts on its own line.
func OpenJournal(path string) (*Journal, State, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o640)
	if err != nil {
		return nil, State{}, err
	}
	j, st, err := openJournal(f, path)
	if err != nil {
		f.Close()
		return nil, State{}, err
	}
	return j, st, nil
}

func openJournal(f *os.File, path string) (*Journal, State, error) {
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, State{}, fmt.Errorf("%w: %s", ErrJournalLocked, path)
		}
		return nil, State{}, err
	}
	data, err := io.ReadAll(f)
	if err != nil {
		return nil, State{}, err
	}
	if len(data) == 0 {
		// A new journal, or one created by an open that crashed before its
		// first record: make its directory entry durable before any record
		// is synced into it. A journal with records had this done already.
		if err := syncDir(filepath.Dir(path)); err != nil {
			return nil, State{}, err
		}
	}
	st, n, end, err := replay(data)
	if err != nil {
		return nil, State{}, err
	}
	if end < len(data) {
		if err := f.Truncate(int64(end)); err != nil {
			return nil, State{}, err
		}
	}
	if _, err := f.Seek(int64(end), io.SeekStart); err != nil {
		return nil, State{}, err
	}
	return &Journal{f: f, seq: n}, st, nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	err = d.Sync()
	if cerr := d.Close(); err == nil {
		err = cerr
	}
	return err
}

// ReplayJournal rebuilds the state a journal describes, reading only.
func ReplayJournal(r io.Reader) (State, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return State{}, err
	}
	st, _, _, err := replay(data)
	return st, err
}

// replay applies every complete line and returns the state, the record count
// and the byte offset just past the last complete line.
func replay(data []byte) (st State, n uint64, end int, err error) {
	for end < len(data) {
		i := bytes.IndexByte(data[end:], '\n')
		if i < 0 {
			return st, n, end, nil // torn tail: never synced, never acted on
		}
		var l journalLine
		dec := json.NewDecoder(bytes.NewReader(data[end : end+i]))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&l); err != nil {
			return st, n, end, fmt.Errorf("%w: line %d: %v", ErrCorrupt, n+1, err)
		}
		if _, err := dec.Token(); err != io.EOF {
			return st, n, end, fmt.Errorf("%w: line %d: trailing data", ErrCorrupt, n+1)
		}
		if l.V != JournalVersion || l.Seq != n+1 {
			return st, n, end, fmt.Errorf("%w: line %d: version %d seq %d", ErrCorrupt, n+1, l.V, l.Seq)
		}
		next, err := Apply(st, l.Rec)
		if err != nil {
			return st, n, end, fmt.Errorf("%w: line %d: %v", ErrCorrupt, n+1, err)
		}
		if next.Digest() != l.State {
			return st, n, end, fmt.Errorf("%w: line %d: state digest differs", ErrCorrupt, n+1)
		}
		st, n, end = next, n+1, end+i+1
	}
	return st, n, end, nil
}

// append writes r with the digest of after and syncs it.
func (j *Journal) append(r Record, after State) error {
	b, err := json.Marshal(journalLine{V: JournalVersion, Seq: j.seq + 1, Rec: r, State: after.Digest()})
	if err != nil {
		return err
	}
	if _, err := j.f.Write(append(b, '\n')); err != nil {
		return err
	}
	if err := j.f.Sync(); err != nil {
		return err
	}
	j.seq++
	return nil
}

// Close closes the journal file.
func (j *Journal) Close() error { return j.f.Close() }

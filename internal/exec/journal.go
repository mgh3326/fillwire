package exec

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
)

// JournalVersion is the journal line format.
const JournalVersion = 1

// ErrCorrupt marks a journal that does not replay: a bad line before the
// last, a sequence gap, a record Apply refuses, or a state digest that does
// not match the one written. The executor must not act on such a journal.
var ErrCorrupt = errors.New("exec: corrupt journal")

type journalLine struct {
	V     int    `json:"v"`
	Seq   uint64 `json:"seq"`
	Rec   Record `json:"rec"`
	State string `json:"state"` // Digest of the state after Rec
}

// Journal is the executor's append-only write-ahead log. A record is synced
// to disk before the executor acts on it.
type Journal struct {
	f   *os.File
	seq uint64
}

// OpenJournal opens or creates the journal at path and replays it. A torn
// last line (a crash during its write; nothing acted on it) is cut off so the
// next record starts on its own line.
func OpenJournal(path string) (*Journal, State, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o640)
	if err != nil {
		return nil, State{}, err
	}
	data, err := io.ReadAll(f)
	if err == nil {
		var st State
		var n uint64
		var end int
		st, n, end, err = replay(data)
		if err == nil && end < len(data) {
			err = f.Truncate(int64(end))
		}
		if err == nil {
			_, err = f.Seek(int64(end), io.SeekStart)
		}
		if err == nil {
			return &Journal{f: f, seq: n}, st, nil
		}
	}
	f.Close()
	return nil, State{}, err
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

package kisfutures

import (
	"archive/zip"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
)

// MasterURL is the public KIS index futures/options master, the file
// auto_trader resolves contracts from. Layout:
// https://github.com/koreainvestment/open-trading-api/blob/main/stocks_info/domestic_index_future_code.py
// It needs no credential.
const MasterURL = "https://new.real.download.dws.co.kr/common/master/fo_idx_code_mts.mst.zip"

const (
	masterEntry    = "fo_idx_code_mts.mst"
	maxMasterBytes = 20_000_000
	masterTimeout  = 15 * time.Second
)

// MasterSource returns the current futures master rows.
type MasterSource interface {
	Master(ctx context.Context) ([]MasterRow, error)
}

// HTTPMaster downloads the master. Redirects are not followed.
type HTTPMaster struct {
	URL    string // empty: MasterURL
	Client *http.Client
}

// Master downloads, unzips and parses the master file.
func (m HTTPMaster) Master(ctx context.Context) ([]MasterRow, error) {
	url := m.URL
	if url == "" {
		url = MasterURL
	}
	client := http.Client{Timeout: masterTimeout}
	if m.Client != nil {
		client = *m.Client
	}
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, errors.New("kisfutures: invalid master URL")
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("kisfutures: master download failed: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("kisfutures: master download answered HTTP %d", response.StatusCode)
	}
	archive, err := readLimited(response.Body)
	if err != nil {
		return nil, err
	}
	reader, err := zip.NewReader(bytes.NewReader(archive), int64(len(archive)))
	if err != nil {
		return nil, fmt.Errorf("kisfutures: master is not a zip archive: %w", err)
	}
	entry, err := reader.Open(masterEntry)
	if err != nil {
		return nil, fmt.Errorf("kisfutures: master archive lacks %s", masterEntry)
	}
	defer entry.Close()
	data, err := readLimited(entry)
	if err != nil {
		return nil, err
	}
	return ParseMaster(data)
}

func readLimited(r io.Reader) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(r, maxMasterBytes+1))
	if err != nil {
		return nil, fmt.Errorf("kisfutures: master read failed: %w", err)
	}
	if len(data) > maxMasterBytes {
		return nil, errors.New("kisfutures: master is too large")
	}
	return data, nil
}

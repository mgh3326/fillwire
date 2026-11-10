package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/mgh3326/fillwire/internal/recording"
	"github.com/mgh3326/fillwire/internal/upbit"
	"github.com/pelletier/go-toml/v2"
)

type recordingFileConfig struct {
	Enabled   bool   `toml:"enabled"`
	Directory string `toml:"directory"`
}
type upbitFileConfig struct {
	Enabled     bool   `toml:"enabled"`
	MarketsFile string `toml:"markets_file"`
	Directory   string `toml:"directory"`
	Endpoint    string `toml:"endpoint"`
	RetryMin    string `toml:"retry_min"`
	RetryMax    string `toml:"retry_max"`
}
type recordingSettings struct {
	enabled   bool
	directory string
}
type upbitSettings struct {
	enabled             bool
	markets             []string
	directory, endpoint string
	retryMin, retryMax  time.Duration
}

func strictTable(contents []byte, name string, target any) (bool, error) {
	var doc map[string]any
	if err := toml.Unmarshal(contents, &doc); err != nil {
		return false, err
	}
	raw, ok := doc[name]
	if !ok {
		return false, nil
	}
	table, ok := raw.(map[string]any)
	if !ok {
		return true, fmt.Errorf("[%s] 표가 아닙니다", name)
	}
	encoded, err := toml.Marshal(table)
	if err != nil {
		return true, err
	}
	d := toml.NewDecoder(bytes.NewReader(encoded))
	d.DisallowUnknownFields()
	if err := d.Decode(target); err != nil {
		return true, fmt.Errorf("[%s] 설정이 잘못되었거나 알 수 없는 키가 있습니다", name)
	}
	return true, nil
}

func resolveRecordingSettings(contents []byte) (recordingSettings, error) {
	var f recordingFileConfig
	present, err := strictTable(contents, "recording", &f)
	if err != nil {
		return recordingSettings{}, err
	}
	if !present || !f.Enabled {
		return recordingSettings{}, nil
	}
	if strings.TrimSpace(f.Directory) == "" {
		return recordingSettings{}, errors.New("recording: 활성화하려면 directory가 필요합니다")
	}
	return recordingSettings{true, f.Directory}, nil
}

func resolveUpbitSettings(contents []byte) (upbitSettings, error) {
	var f upbitFileConfig
	present, err := strictTable(contents, "upbit", &f)
	if err != nil {
		return upbitSettings{}, err
	}
	if !present || !f.Enabled {
		return upbitSettings{}, nil
	}
	if strings.TrimSpace(f.MarketsFile) == "" || strings.TrimSpace(f.Directory) == "" {
		return upbitSettings{}, errors.New("upbit: 활성화하려면 markets_file과 directory가 필요합니다")
	}
	raw, err := os.ReadFile(f.MarketsFile)
	if err != nil {
		return upbitSettings{}, fmt.Errorf("upbit: markets_file 읽기 실패: %w", err)
	}
	markets := []string{}
	for _, line := range strings.Split(string(raw), "\n") {
		m := strings.TrimSpace(line)
		if m == "" || strings.HasPrefix(m, "#") {
			continue
		}
		markets = append(markets, m)
	}
	if len(markets) < 1 || len(markets) > 20 {
		return upbitSettings{}, errors.New("upbit: 마켓 파일에는 1~20개 마켓이 있어야 합니다")
	}
	for _, m := range markets {
		if !validKRWMarket(m) {
			return upbitSettings{}, errors.New("upbit: KRW 마켓만 사용할 수 있습니다")
		}
	}
	retryMin := time.Second
	retryMax := 30 * time.Second
	if f.RetryMin != "" {
		retryMin, err = time.ParseDuration(f.RetryMin)
		if err != nil || retryMin <= 0 {
			return upbitSettings{}, errors.New("upbit: retry_min 설정이 잘못되었습니다")
		}
	}
	if f.RetryMax != "" {
		retryMax, err = time.ParseDuration(f.RetryMax)
		if err != nil || retryMax < retryMin {
			return upbitSettings{}, errors.New("upbit: retry_max 설정이 잘못되었습니다")
		}
	}
	if err := upbit.ValidateMarkets(markets); err != nil {
		return upbitSettings{}, err
	}
	return upbitSettings{enabled: true, markets: markets, directory: f.Directory, endpoint: f.Endpoint, retryMin: retryMin, retryMax: retryMax}, nil
}

func validKRWMarket(m string) bool {
	if !strings.HasPrefix(m, "KRW-") || len(m) < 6 || len(m) > 24 {
		return false
	}
	for _, r := range m[4:] {
		if !(r >= 'A' && r <= 'Z') && !(r >= '0' && r <= '9') {
			return false
		}
	}
	return true
}

type upbitLane struct {
	stop context.CancelFunc
	done chan struct{}
}

func startUpbitLane(settings upbitSettings, logger *slog.Logger) upbitLane {
	if !settings.enabled {
		return upbitLane{}
	}
	reader, err := upbit.New(upbit.Config{Markets: settings.markets, Root: settings.directory, Endpoint: settings.endpoint, RetryMin: settings.retryMin, RetryMax: settings.retryMax, Logger: logger.With("lane", "upbit")})
	if err != nil {
		logger.Error("Upbit 기록기를 시작하지 못했습니다", "reason", err.Error())
		return upbitLane{}
	}
	ctx, stop := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := reader.Run(ctx); err != nil && ctx.Err() == nil {
			logger.Error("Upbit 기록기가 중지되었습니다", "reason", err.Error())
		}
	}()
	return upbitLane{stop: stop, done: done}
}
func (l upbitLane) halt() {
	if l.stop != nil {
		l.stop()
		<-l.done
	}
}

func runCoverageCommand(args []string) error {
	fs := flag.NewFlagSet("coverage", flag.ContinueOnError)
	dir := fs.String("dir", "", "기록 루트 디렉터리")
	day := fs.String("day", "", "UTC 날짜 YYYY-MM-DD")
	from := fs.String("from", "", "선택적 커버리지 구간 시작 시각 (RFC3339)")
	to := fs.String("to", "", "선택적 커버리지 구간 종료 시각 (RFC3339)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *dir == "" || *day == "" {
		return errors.New("usage: 사용법: fillwire coverage --dir <directory> --day YYYY-MM-DD [--from RFC3339 --to RFC3339]")
	}
	var rows any
	var err error
	if *from != "" || *to != "" {
		if *from == "" || *to == "" {
			return errors.New("coverage: --from과 --to를 함께 지정해야 합니다")
		}
		a, e := time.Parse(time.RFC3339, *from)
		if e != nil {
			return e
		}
		b, e := time.Parse(time.RFC3339, *to)
		if e != nil {
			return e
		}
		rows, err = recording.SummarizeWindow(*dir, a, b)
	} else {
		rows, err = recording.SummarizeDay(*dir, *day)
	}
	if err != nil {
		return err
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(rows)
}

func runUpbitCommand(args []string) error {
	fs := flag.NewFlagSet("upbit", flag.ContinueOnError)
	marketsFile := fs.String("markets-file", "", "한 줄에 KRW 마켓 코드 하나씩 지정")
	dir := fs.String("dir", "", "기록 루트 디렉터리")
	endpoint := fs.String("endpoint", upbit.Endpoint, "공개 웹소켓 주소")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *marketsFile == "" || *dir == "" {
		return errors.New("usage: 사용법: fillwire upbit --markets-file <path> --dir <directory>")
	}
	raw, err := os.ReadFile(*marketsFile)
	if err != nil {
		return err
	}
	markets := []string{}
	for _, line := range strings.Split(string(raw), "\n") {
		m := strings.TrimSpace(line)
		if m != "" && !strings.HasPrefix(m, "#") {
			markets = append(markets, m)
		}
	}
	reader, err := upbit.New(upbit.Config{Markets: markets, Root: *dir, Endpoint: *endpoint, Logger: slog.New(slog.NewTextHandler(os.Stderr, nil))})
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return reader.Run(ctx)
}

// File: cmd/preset/main.go
// Package: main
//
// 職責 (Responsibility):
//     提供 preset loader 的 CLI 入口：
//       - 讀取 configs/system_presets 下的 YAML
//       - 編譯為各部門可直接使用的 packets
//       - 以 JSON 輸出（供 Python / shell / CI 使用）
//
// 注意事項 (Notes):
//     - 本程式不做 adapter capability table 驗證；僅做最小格式與語意檢查。
//     - 若 target=all，輸出為單一 JSON 物件，欄位為 market_data / instrument_data / trading_allowlist。

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"dorma_system/infra/preset"
)

type targetKind string

const (
	targetMarketData     targetKind = "market_data"
	targetInstrumentData targetKind = "instrument_data"
	targetTradingAllow   targetKind = "trading_allowlist"
	targetAll            targetKind = "all"
)

type allOut struct {
	MarketData     any `json:"market_data,omitempty"`
	InstrumentData any `json:"instrument_data,omitempty"`
	TradingAllow   any `json:"trading_allowlist,omitempty"`
}

func main() {
	var (
		target = flag.String("target", "all", "target packet: market_data | instrument_data | trading_allowlist | all")
		pretty = flag.Bool("pretty", false, "pretty-print JSON")
		out    = flag.String("out", "", "output file path (default: stdout)")
		quiet  = flag.Bool("quiet", false, "suppress stderr logs (only output JSON)")
	)
	flag.Parse()

	tk, err := parseTarget(*target)
	if err != nil {
		fatalf(2, *quiet, "%v\n", err)
	}

	var payload any
	switch tk {
	case targetMarketData:
		p, err := preset.LoadMarketDataPacket()
		if err != nil {
			fatalf(1, *quiet, "load market_data packet: %v\n", err)
		}
		payload = p

	case targetInstrumentData:
		p, err := preset.LoadInstrumentDataPacket()
		if err != nil {
			fatalf(1, *quiet, "load instrument_data packet: %v\n", err)
		}
		payload = p

	case targetTradingAllow:
		p, err := preset.LoadTradingAllowlistPacket()
		if err != nil {
			fatalf(1, *quiet, "load trading_allowlist packet: %v\n", err)
		}
		payload = p

	case targetAll:
		p, err := preset.LoadAllPackets()
		if err != nil {
			fatalf(1, *quiet, "load all packets: %v\n", err)
		}
		payload = allOut{
			MarketData:     p.MarketData,
			InstrumentData: p.InstrumentData,
			TradingAllow:   p.TradingAllow,
		}

	default:
		fatalf(2, *quiet, "unknown target: %s\n", tk)
	}

	var b []byte
	if *pretty {
		b, err = json.MarshalIndent(payload, "", "  ")
	} else {
		b, err = json.Marshal(payload)
	}
	if err != nil {
		fatalf(1, *quiet, "marshal json: %v\n", err)
	}
	b = append(b, '\n')

	w, closeFn, err := openOut(*out)
	if err != nil {
		fatalf(1, *quiet, "open output: %v\n", err)
	}
	if closeFn != nil {
		defer closeFn()
	}

	if !*quiet && *out != "" {
		_, _ = fmt.Fprintf(os.Stderr, "preset: wrote %s\n", *out)
	}
	if _, err := w.Write(b); err != nil {
		fatalf(1, *quiet, "write output: %v\n", err)
	}
}

func parseTarget(s string) (targetKind, error) {
	v := targetKind(strings.TrimSpace(strings.ToLower(s)))
	switch v {
	case targetMarketData, targetInstrumentData, targetTradingAllow, targetAll:
		return v, nil
	default:
		return "", fmt.Errorf("invalid -target=%q (expected: market_data|instrument_data|trading_allowlist|all)", s)
	}
}

func openOut(path string) (io.Writer, func() error, error) {
	if strings.TrimSpace(path) == "" {
		return os.Stdout, nil, nil
	}
	f, err := os.Create(path)
	if err != nil {
		return nil, nil, err
	}
	return f, f.Close, nil
}

func fatalf(code int, quiet bool, format string, args ...any) {
	if !quiet {
		_, _ = fmt.Fprintf(os.Stderr, format, args...)
	}
	os.Exit(code)
}

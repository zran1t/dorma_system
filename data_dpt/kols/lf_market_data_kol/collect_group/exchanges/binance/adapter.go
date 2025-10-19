// data_dpt/kols/lf_market_data_kol/collect_group/exchanges/binance/adapter.go
package binance

import (
	"fmt"
	"time"

	"dorma_system/infra/transport/ws"
)

// NewAdapter 回傳一個暫時的骨架 Adapter（尚未支援）
func NewAdapter() ws.Adapter { return &adapter{} }

type adapter struct{}

func (a *adapter) BuildSubscribeMsgs(channels, symbols []string) ([]string, error) {
	// 先保留接口；未來要做 Binance KLine（premium index / mark）的 candle 再補
	return nil, fmt.Errorf("binance kline adapter not implemented yet")
}

func (a *adapter) Heartbeat() (string, time.Duration) { return "", 0 }

func (a *adapter) Handle(_ []byte) (string, []byte, error) {
	// 不處理任何資料
	return "", nil, nil
}
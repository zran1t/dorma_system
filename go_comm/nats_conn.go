// data_dpt/go_comm/nats_conn.go
// 說明: NATS 連線管理（Core NATS，無 JetStream）
// 功能: 封裝連線建立與基本重連設定
// 最後修改: 2025-09-12 by AI Writer (Ticket ID: GO-COMM-CONN-0001)

package go_comm

import (
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
)

// ConnectNATS
// 說明: 建立到 NATS 的連線（含自動重連）；成功回傳 *nats.Conn
// 參數: natsURL 例如 "nats://127.0.0.1:4222"
// 錯誤: 無效位址 / 連線失敗時回傳具體錯誤（中文）
func ConnectNATS(natsURL string) (*nats.Conn, error) {
	if natsURL == "" {
		return nil, errors.New("NATS 位址不可為空")
	}
	nc, err := nats.Connect(
		natsURL,
		nats.Name("data-dpt-go"), // 連線名稱方便監控
		nats.MaxReconnects(-1),   // 無限重連
		nats.ReconnectWait(250*time.Millisecond),
		nats.DrainTimeout(5*time.Second),
		nats.NoEcho(), // 減少自反訊息
	)
	if err != nil {
		return nil, fmt.Errorf("連線 NATS 失敗: %w", err)
	}
	return nc, nil
}

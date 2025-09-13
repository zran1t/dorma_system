// data_dpt/go_comm/nats_pub.go
// 說明: NATS 發佈抽象層（高頻協定 v1 封裝）
// 功能: 依 pub_tpl 組 subject → 將 Protobuf Envelope 發佈到 NATS
// 依據: configs/channels/market.yaml → pub_tpl: "market.{venue}.{symbol}.{feed}"
// 最後修改: 2025-09-12 by AI Writer (Ticket ID: GO-COMM-PUB-0002)

package go_comm

import (
	"errors"
	"fmt"
	"strings"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"

	pb "dorma_system/schemas/gen/go"
)

// --- 內部小工具 ---

// formatSubject: 依 pub_tpl 與參數組出 subject
// 例: "market.{venue}.{symbol}.{feed}" → "market.BINANCE.BTCUSDT.trades"
func formatSubject(pubTpl, venue, symbol, feedToken string) (string, error) {
	if pubTpl == "" {
		return "", errors.New("pub_tpl 不可為空")
	}
	if venue == "" || symbol == "" || feedToken == "" {
		return "", errors.New("venue/symbol/feed 不可為空")
	}
	s := strings.ReplaceAll(pubTpl, "{venue}", venue)
	s = strings.ReplaceAll(s, "{symbol}", symbol)
	s = strings.ReplaceAll(s, "{feed}", strings.ToLower(feedToken))
	if strings.Contains(s, "{") || strings.Contains(s, "}") {
		return "", fmt.Errorf("pub_tpl 仍包含未替換占位符: %s", s)
	}
	return s, nil
}

// feedToToken: 將枚舉轉成 subject 的末段字串
func feedToToken(ft pb.FeedType) (string, error) {
	switch ft {
	case pb.FeedType_FEED_TRADES:
		return "trades", nil
	case pb.FeedType_FEED_BBO:
		return "bbo", nil
	case pb.FeedType_FEED_BBT:
		return "bbt", nil
	default:
		return "", fmt.Errorf("不支援的 FeedType: %v", ft)
	}
}

// --- 對外介面 ---

// PublishEnvelope
// 說明: 將 payload(Trades/Bbo/Bbt) 打包為 Envelope 後，依 pub_tpl 發佈
// 參數:
//   - nc: 已連線的 *nats.Conn（由 ConnectNATS 取得）
//   - pubTpl: 例 "market.{venue}.{symbol}.{feed}"
//   - venue/symbol: 例 "BINANCE" / "BTCUSDT"
//   - feed: pb.FeedType（FEED_TRADES / FEED_BBO / FEED_BBT）
//   - src: 來源代碼（無則 0）
//   - msg: 對應 feed 的 protobuf 訊息（*pb.Trades / *pb.Bbo / *pb.Bbt）
//
// 錯誤: 任一步驟失敗回傳中文錯誤
func PublishEnvelope(nc *nats.Conn, pubTpl, venue, symbol string, feed pb.FeedType, src uint32, msg proto.Message) error {
	if nc == nil || !nc.IsConnected() {
		return errors.New("NATS 連線無效或未連線")
	}

	// 1) 型別檢查 + 序列化 payload
	var body []byte
	switch feed {
	case pb.FeedType_FEED_TRADES:
		if _, ok := msg.(*pb.Trades); !ok {
			return fmt.Errorf("參數錯誤: 期待 *pb.Trades，實得 %T", msg)
		}
	case pb.FeedType_FEED_BBO:
		if _, ok := msg.(*pb.Bbo); !ok {
			return fmt.Errorf("參數錯誤: 期待 *pb.Bbo，實得 %T", msg)
		}
	case pb.FeedType_FEED_BBT:
		if _, ok := msg.(*pb.Bbt); !ok {
			return fmt.Errorf("參數錯誤: 期待 *pb.Bbt，實得 %T", msg)
		}
	default:
		return fmt.Errorf("不支援的 FeedType: %v", feed)
	}
	var err error
	body, err = proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("序列化 payload 失敗: %w", err)
	}

	// 2) 包成 Envelope
	env := &pb.Envelope{
		Ver:  pb.Version_V1,
		Feed: feed,
		Src:  src,
		Body: body,
	}
	wire, err := proto.Marshal(env)
	if err != nil {
		return fmt.Errorf("序列化 Envelope 失敗: %w", err)
	}

	// 3) 組 subject 並發佈
	feedToken, err := feedToToken(feed)
	if err != nil {
		return err
	}
	subject, err := formatSubject(pubTpl, venue, symbol, feedToken)
	if err != nil {
		return err
	}
	if err := nc.Publish(subject, wire); err != nil {
		return fmt.Errorf("發佈失敗 subject=%s: %w", subject, err)
	}
	return nil
}

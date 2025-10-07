package ws

import (
	"context"
	"dorma_system/infra/pubsub"
	"log"
	"net/http"
	"time"
)

type CollectorConfig struct {
	URL              string        // ws 端點網址
	Headers          http.Header   // 連線的header 放token之類的 身份驗證
	Channels         []string      // 資料頻道
	Symbols          []string      // 標的物
	PingEvery        time.Duration // Ping的間隔
	ReconnectBackoff time.Duration // 斷線重連間隔
}

type Collector struct {
	ws   WSClient 			// Connect,SendJSON,Recv,Close WS的抽象基底層 連線 發送訊息 接收訊息 關閉連線
	adj  Adapter			// BuildSubscribeMsgs,Heartbeat,Handle 交易所抽象基底層 構建訂閱封包 心跳 接收回傳
	pub  pubsub.Bus	// Publisher 推送nats
	conf CollectorConfig	// Collector 設定
} 

// NewCollector 建立一個 Collector 實例
// - ws:   WebSocket 客戶端（例如 GorillaWS 實作 WSClient 介面）
// - adj:  Adapter，決定交易所的訂閱封包/心跳/解析邏輯
// - pub:  Publisher，決定資料要送去哪裡（例如 NATS）
// - conf: CollectorConfig，基本設定（URL、頻道、symbols 等）
//
// 如果沒有特別設定 ReconnectBackoff，預設為 3 秒。
// 回傳的 Collector 實例可以用 Connect/Subscribe/Run 來跑收集流程。
func NewCollector(ws WSClient, adj Adapter, pub pubsub.Bus, conf CollectorConfig) *Collector {
	if conf.ReconnectBackoff <= 0 {
		conf.ReconnectBackoff = 3 * time.Second
	}
	return &Collector{ws: ws, adj: adj, pub: pub, conf: conf}
}

// 呼叫抽象基底層與ws端點建立連線
func (c *Collector) Connect(ctx context.Context) error {
	return c.ws.Connect(ctx, c.conf.URL, c.conf.Headers)
}

// 訂閱ws頻道也就是傳入訂閱封包
// 內容用到BuildSubscribeMsgs 建立訂閱封包的介面規範
func (c *Collector) Subscribe(ctx context.Context) error {
	msgs, err := c.adj.BuildSubscribeMsgs(c.conf.Channels, c.conf.Symbols)
	if err != nil {
		return err
	}
	for _, m := range msgs { // 一個一個封包取出
		if err := c.ws.SendJSON(ctx, m); err != nil { // 一個一個傳送訂閱 
			return err
		}
		time.Sleep(20 * time.Millisecond) // 輕節流
	}
	return nil
}

func (c *Collector) Run(ctx context.Context) error {
	// 心跳
	hbPayload, hbEvery := c.adj.Heartbeat()
	if hbEvery == 0 && c.conf.PingEvery > 0 { // 交易所不需要主動ping 就用設定裡的PingEvery
		hbEvery = c.conf.PingEvery
	}
	var hbTicker *time.Ticker
	if hbEvery > 0 {  // 交易所有規定要主動ping
		hbTicker = time.NewTicker(hbEvery) // 設定每隔hbEvery就滴答一次
		defer hbTicker.Stop()
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		default: // 如果還收到生命週期 Done 就會走到 default 下面是空的 這是為了不讓他卡死保護
		}

		// 非阻塞心跳
		if hbTicker != nil { // 若有心跳（這裡理論上一定有）
			select {
			case <-hbTicker.C: // 讀ticker是否剛好到點 到的話就執行下方 送出心跳
				if hbPayload != "" {
					_ = c.ws.SendJSON(ctx, hbPayload)
				} else {
					_ = c.ws.SendPing(ctx)
				}
			default: // 偵測ticker計時器沒到就會進空的default避免卡死
			}
		}

		msgType, data, err := c.ws.Recv(ctx) // 阻塞住整個迴圈等待接收到訊息往下走

		if err != nil { // 若出現錯誤訊息
			log.Printf("[collector] 接收訊息錯誤: %v -> 重新連線中...", err)
			_ = c.ws.Close() // 先關閉連線 確保無殘留
			select {
			case <-time.After(c.conf.ReconnectBackoff): // 等待設定的重新連線時間 繼續往下走
			case <-ctx.Done(): // 若偵測到是主動結束 就直結return出去停止回圈
				return nil
			}
			// 重新連線間隔到了後執行連線和訂閱 若還是出錯 就不阻塞直接continue跳出本輪迴圈 重新嘗試一次
			if err := c.Connect(ctx); err != nil {
				continue
			}
			if err := c.Subscribe(ctx); err != nil {
				continue
			}
			continue // 若兩個動作都成功一樣跳到下一輪迴圈 去接收訊息 
		}

		// 正常情況下會走到這裡：處理收到的 WS 訊息
		if msgType == 1 /* websocket.TextMessage */ { // 1=Text frame（通常是 JSON）
			// 交給交易所 Adapter 做解析/路由
			// 回傳：subject（要發佈到哪個題目）、body（發佈的 payload）
			subject, body, err := c.adj.Handle(data)
			if err != nil {
				log.Printf("[collector] 處理訊息錯誤: %v", err)
				continue // 單包丟棄，主循環續跑
			}
			// 有主題且有內容才發佈
			if subject != "" && len(body) > 0 {
				if err := c.pub.Publish(ctx, subject, body); err != nil {
					log.Printf("[collector] 發佈失敗: %v", err)
				}
			}
		}
	}
}

// 對抽象基底層Clos的二次封裝
func (c *Collector) Close() error { return c.ws.Close() }

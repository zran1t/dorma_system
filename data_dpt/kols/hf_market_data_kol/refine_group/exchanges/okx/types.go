// types.go
package okx

// OKX trades-all 外層/內層欄位
// 例：{"arg":{"channel":"trades-all","instId":"BTC-USDT-SWAP"},"data":[{...}]}
type okxTradesAllHead struct {
	Arg struct {
		Channel string `json:"channel"`
		InstID  string `json:"instId"`
	} `json:"arg"`
	Data []struct {
		TradeID string `json:"tradeId"`
		Px      string `json:"px"`
		Sz      string `json:"sz"`
		Side    string `json:"side"`
		TS      string `json:"ts"`
	} `json:"data"`
}

// OKX trades（聚合版）外層/內層欄位
type okxTradesHead struct {
	Arg struct {
		Channel string `json:"channel"`
		InstID  string `json:"instId"`
	} `json:"arg"`
	Data []struct {
		SeqID   uint64 `json:"seqId"`
		Count   uint32 `json:"count,string"`
		TradeID string `json:"tradeId"`
		Px      string `json:"px"`
		Sz      string `json:"sz"`
		Side    string `json:"side"`
		TS      string `json:"ts"`
	} `json:"data"`
}

// OKX bbo 外層/內層欄位
type okxBBOHead struct {
	Arg struct {
		Channel string `json:"channel"`
		InstID  string `json:"instId"`
	} `json:"arg"`
	Data []struct {
		BidPx  string `json:"bidPx"`
		BidSz  string `json:"bidSz"`
		AskPx  string `json:"askPx"`
		AskSz  string `json:"askSz"`
		BidCnt uint32 `json:"bidOrdCt"`
		AskCnt uint32 `json:"askOrdCt"`
		SeqID  uint64 `json:"seqId"`
		TS     string `json:"ts"`
	} `json:"data"`
}

// OKX books 外層/內層欄位（snapshot/update）
type okxBookHead struct {
	Arg struct {
		Channel string `json:"channel"`
		InstID  string `json:"instId"`
	} `json:"arg"`
	Action string `json:"action"` // "snapshot" or "update"
	Data   []struct {
		Asks     [][]string `json:"asks"` // [[px, qty, ...], ...]
		Bids     [][]string `json:"bids"`
		Checksum int32     `json:"checksum"`
		TS       string     `json:"ts"`
		PrevSeqID int64    `json:"prevSeqId"`
		SeqID     uint64    `json:"seqId"`
	} `json:"data"`
}


// ---- head structs（比照你現有 head 結構的風格）----
type okxMarkPriceHead struct {
    Arg struct {
        Channel string `json:"channel"`
        InstID  string `json:"instId"`
    } `json:"arg"`
    Data []struct {
        InstType string `json:"instType"`
        InstID   string `json:"instId"`
        MarkPx   string `json:"markPx"`
        TS       string `json:"ts"`
    } `json:"data"`
}

type okxIndexTickersHead struct {
    Arg struct {
        Channel string `json:"channel"`
        InstID  string `json:"instId"`
    } `json:"arg"`
    Data []struct {
        InstID  string `json:"instId"`
        IdxPx   string `json:"idxPx"`
        High24h string `json:"high24h"`
        Low24h  string `json:"low24h"`
        Open24h string `json:"open24h"`
        SodUtc0 string `json:"sodUtc0"`
        SodUtc8 string `json:"sodUtc8"`
        TS      string `json:"ts"`
    } `json:"data"`
}
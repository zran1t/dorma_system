// data_dpt/kols/lf_market_data_kol/refine_group/exchanges/okx/types.go
package okx

// OKX kline 外層/內層欄位（mark/index 共用結構；data 為多列 OHLC）
type okxCandleHead struct {
	Arg struct {
		Channel string `json:"channel"` // e.g. "mark-price-candle1D", "index-candle1Dutc"
		InstID  string `json:"instId"`
	} `json:"arg"`
	Data [][]string `json:"data"`
}

// data[i] 欄位：
// [0]=ts(ms), [1]=o, [2]=h, [3]=l, [4]=c, [5]=confirm("0"/"1")
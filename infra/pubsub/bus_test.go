// File: infra/pubsub/bus_test.go
// Package: pubsub
//
// 職責 (Responsibility):
//
//	測試 pubsub 套件中「純邏輯」的部分，
//	包含：
//	  - Handler 型別簽名編譯期檢查。
//	  - NATSCoreBus 是否實作預期介面（編譯期檢查）。
//	  - NATS URL 驗證器（validateNATSURLList）的輸入規則。
//	不會對外連線、不依賴 NATS server。
package pubsub

import (
	"context"
	"testing"
)

// TestHandler_Signature_CompileTimeCheck
//
// 功能:
//   - 用編譯器幫忙檢查 Handler 型別簽名是否符合預期。
//   - 沒有 runtime 行為，重點是「只要編得過就代表簽名沒問題」。
//
// 說明:
//   - 若未來不小心改到 Handler 型別，這裡會直接編譯失敗。
func TestHandler_Signature_CompileTimeCheck(t *testing.T) {
	f := func(ctx context.Context, m *Message) error {
		_ = ctx
		_ = m
		return nil
	}
	var _ Handler = f
}

// TestNATSCoreBus_ImplementsInterfaces
//
// 功能:
//   - 用編譯器檢查 NATSCoreBus 是否實作 Bus / Publisher / Subscriber 三個介面。
//   - 若哪天改了 NATSCoreBus 的方法簽名導致介面不相容，這裡會直接編譯失敗。
func TestNATSCoreBus_ImplementsInterfaces(t *testing.T) {
	var _ Bus = (*NATSCoreBus)(nil)
	var _ Publisher = (*NATSCoreBus)(nil)
	var _ Subscriber = (*NATSCoreBus)(nil)
}

// TestValidateNATSURLList
//
// 功能:
//   - 驗證 validateNATSURLList 對常見輸入的接受/拒絕行為符合預期。
//
// 說明:
//   - 這是純邏輯測試，不會發起任何網路連線。
//   - 若未來你收緊/放寬 URL 規則，請同步調整此測試。
func TestValidateNATSURLList(t *testing.T) {
	cases := []struct {
		name    string
		raw     string
		wantErr bool
	}{
		{
			name:    "empty string",
			raw:     "",
			wantErr: true,
		},
		{
			name:    "single nats url ok",
			raw:     "nats://127.0.0.1:4222",
			wantErr: false,
		},
		{
			name:    "single tls url ok",
			raw:     "tls://127.0.0.1:4222",
			wantErr: false,
		},
		{
			name:    "unsupported scheme",
			raw:     "http://127.0.0.1:4222",
			wantErr: true,
		},
		{
			name:    "host empty",
			raw:     "nats://",
			wantErr: true,
		},
		{
			name:    "comma separated multi urls ok",
			raw:     "nats://127.0.0.1:4222, nats://localhost:4222",
			wantErr: false,
		},
		{
			name:    "comma separated contains empty entry",
			raw:     "nats://127.0.0.1:4222, , nats://localhost:4222",
			wantErr: true,
		},
		{
			name:    "whitespace only",
			raw:     "   ",
			wantErr: true,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			err := validateNATSURLList(tc.raw)
			if tc.wantErr && err == nil {
				t.Fatalf("預期錯誤，但得到 nil (raw=%q)", tc.raw)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("預期成功，但得到錯誤: %v (raw=%q)", err, tc.raw)
			}
		})
	}
}

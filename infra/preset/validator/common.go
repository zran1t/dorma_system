// File: infra/preset/validator/common.go
// Package: validator
//
// 職責 (Responsibility):
//     提供 validator 共用錯誤與基本檢查工具。
//
// 注意事項 (Notes):
//     - capability 驗證（ex 支援哪些 feeds）不在此處實作；這裡僅做格式與最小語意檢查。

package validator

import "fmt"

func errf(format string, args ...any) error {
	return fmt.Errorf(format, args...)
}

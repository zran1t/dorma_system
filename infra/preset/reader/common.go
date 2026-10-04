// File: infra/preset/reader/common.go
// Package: reader
//
// 職責 (Responsibility):
//     提供 reader 共用的 YAML I/O 與反序列化工具。
//     reader 僅負責 I/O 與資料承載，不做任何推導、合併或驗證。

package reader

import (
	"os"

	"gopkg.in/yaml.v3"
)

func readYAMLFile(path string, out any) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return yaml.Unmarshal(b, out)
}

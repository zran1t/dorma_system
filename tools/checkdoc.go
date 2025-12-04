package main

import (
	"bufio"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
)

var (
	// 要忽略的資料夾（以目錄名稱比對）
	ignoreDirs = map[string]struct{}{
		".git":         {},
		"vendor":       {},
		"gen":          {},
		"schemas/gen":  {}, // 你專案的 proto 產物路徑（可依需要調整/刪掉）
		"node_modules": {},
	}

	// 統計
	filesScanned int
	funcsScanned int
	warnCount    int
	errorCount   int
)

// ANSI 顏色
const (
	colorReset  = "\033[0m"
	colorRed    = "\033[31m"
	colorYellow = "\033[33m"
	colorGreen  = "\033[32m"
)

func logError(format string, a ...any) {
	errorCount++
	fmt.Printf(colorRed+"[ERROR] "+colorReset+format+"\n", a...)
}

func logWarn(format string, a ...any) {
	warnCount++
	fmt.Printf(colorYellow+"[WARN]  "+colorReset+format+"\n", a...)
}

// 判斷是否為自動產生檔案：偵測標頭 "Code generated"（Go 工具慣例）
func isCodeGenerated(path string) bool {
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	for i := 0; i < 20 && sc.Scan(); i++ { // 只看前 20 行
		line := sc.Text()
		if strings.Contains(line, "Code generated") || strings.Contains(line, "DO NOT EDIT") {
			return true
		}
	}
	return false
}

// 目錄是否應忽略（名稱或前綴）
func shouldIgnoreDir(path string) bool {
	base := filepath.Base(path)
	if _, ok := ignoreDirs[base]; ok {
		return true
	}
	// prefix 型（像 schemas/gen 這種層級）
	for p := range ignoreDirs {
		if strings.HasPrefix(path, p) {
			return true
		}
	}
	return false
}

func checkFile(path string) {
	if isCodeGenerated(path) {
		return
	}

	fset := token.NewFileSet()
	node, err := parser.ParseFile(fset, path, nil, parser.ParseComments)
	if err != nil {
		logWarn("無法解析 %s: %v", path, err) // 解析失敗視為警告，不阻塞
		return
	}

	filesScanned++

	for _, decl := range node.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok {
			continue
		}
		// 忽略測試檔
		//if strings.HasSuffix(path, "_test.go") {
		//	continue
		//}
		// 忽略 protobuf 產物（檔名包含 pb.go）
		if strings.HasSuffix(path, ".pb.go") {
			continue
		}

		funcsScanned++
		pos := fset.Position(fn.Pos())
		funcName := fn.Name.Name

		// 1) 是否有 docstring
		if fn.Doc == nil || len(fn.Doc.List) == 0 {
			logError("%s:%d: 函式 %s 缺少註解", path, pos.Line, funcName)
			continue
		}

		// 2) 第一行是否以函式名開頭（GoDoc 推薦樣式）
		first := strings.TrimSpace(strings.TrimPrefix(fn.Doc.List[0].Text, "//"))
		if !strings.HasPrefix(first, funcName) {
			logWarn("%s:%d: 註解未以函式名開頭（建議：'%s ...'）", path, pos.Line, funcName)
		}
	}
}

func main() {
	root := "."

	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			// 取不到檔案資訊就略過，不中斷
			logWarn("略過路徑（權限/錯誤）：%s (%v)", path, err)
			return nil
		}
		if info.IsDir() {
			if shouldIgnoreDir(path) {
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) == ".go" {
			checkFile(path)
		}
		return nil
	})
	if err != nil {
		logError("掃描發生致命錯誤: %v", err)
		os.Exit(2)
	}

	// 總結
	fmt.Println()
	fmt.Printf(colorGreen + "[DONE] " + colorReset + "掃描完成\n")
	fmt.Printf("  檔案：%d  函式：%d\n", filesScanned, funcsScanned)
	fmt.Printf("  %s錯誤：%d%s  %s警告：%d%s\n",
		colorRed, errorCount, colorReset,
		colorYellow, warnCount, colorReset,
	)

	// 有錯誤時回傳非零，方便 CI fail
	if errorCount > 0 {
		os.Exit(1)
	}
}

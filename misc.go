// unkodb
// author: Leonardone @ NEETSDKASU

package unkodb

import (
	"fmt"
)

// ジェネリック難しいね
// https://go.dev/blog/intro-generics
// 上記ページによると
// golang.org/x/exp/constraints パッケージの constraints.Integer を使うと
// int系uint系および派生(?)型（`type Foo int32`みたいなやつ？)を扱えるらしい
// ※派生は型にチルダの記号が付くと対応するらしい ~int32 とすると `type Foo int32` の Foo も対象にできる、とかぽい？
// パッケージ詳細 https://pkg.go.dev/golang.org/x/exp/constraints
type integerTypes interface {
	int8 | uint8 | int16 | uint16 | int32 | uint32 | int64 | uint64
}

var bug = struct {
	Panic  func(v any)
	Panicf func(format string, v ...any)
}{
	Panic: func(v any) {
		if e, ok := v.(error); ok {
			panic(fmt.Errorf("[BUG] %w", e))
		} else {
			panic(fmt.Errorf("[BUG] %v", v))
		}
	},
	Panicf: func(format string, v ...any) {
		panic(fmt.Errorf("[BUG] "+format, v...))
	},
}

func catchError(err *error) {
	if v := recover(); v != nil {
		if e, ok := v.(error); ok {
			*err = e
		} else {
			*err = fmt.Errorf("ERROR! %#v", v)
		}
	}
}

func minValue(a, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

// unkodb
// author: Leonardone @ NEETSDKASU

package unkodb

import (
	"github.com/neetsdkasu/avltree"
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

// 本当は
// type geneKey[T integerTypes] T
//　としたかったけど、ダメぽい
type geneKey[T integerTypes] struct {
	value T
}

func intKey[T integerTypes](value T) *geneKey[T] {
	return &geneKey[T]{value: value}
}

func (key *geneKey[T]) CompareTo(other avltree.Key) (_ avltree.KeyOrdering) {
	if x, ok := other.(*geneKey[T]); ok {
		switch {
		case key.value < x.value:
			return avltree.LessThanOtherKey
		case key.value > x.value:
			return avltree.GreaterThanOtherKey
		default:
			return avltree.EqualToOtherKey
		}
	} else {
		logger.Panicf("[BUG] invalid key type (key: %T %#v)", other, other)
		return
	}
}

func (key *geneKey[T]) Copy() avltree.Key {
	return intKey[T](key.value)
}

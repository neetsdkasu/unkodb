// unkodb
// author: Leonardone @ NEETSDKASU

package unkodb

import (
	"github.com/neetsdkasu/avltree"
)

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
		panicf("[BUG] invalid key type (key: %T %#v)", other, other)
		return
	}
}

func (key *geneKey[T]) Copy() avltree.Key {
	return intKey[T](key.value)
}

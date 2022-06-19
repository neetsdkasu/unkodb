// unkodb
// author: Leonardone @ NEETSDKASU

package unkodb

var (
	_ Column = &intColumn[int8]{}
	_ Column = &intColumn[uint8]{}
	_ Column = &intColumn[int16]{}
	_ Column = &intColumn[uint16]{}
	_ Column = &intColumn[int32]{}
	_ Column = &intColumn[uint32]{}
	_ Column = &intColumn[int64]{}
	_ Column = &intColumn[uint64]{}
	_ Column = &shortStringColumn{}
)

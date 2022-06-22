// unkodb
// author: Leonardone @ NEETSDKASU

package unkodb

type Table struct {
	name         string
	key          Column
	columns      []Column
	rootAccessor rootAddressAccessor
}

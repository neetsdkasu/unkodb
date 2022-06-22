// unkodb
// author: Leonardone @ NEETSDKASU

package unkodb

import (
	"testing"
)

func TestColumn(t *testing.T) {
	columns := []Column{
		&intColumn[int8]{},
		&intColumn[uint8]{},
		&intColumn[int16]{},
		&intColumn[uint16]{},
		&intColumn[int32]{},
		&intColumn[uint32]{},
		&intColumn[int64]{},
		&intColumn[uint64]{},
		&shortStringColumn{},
		&shortBytesColumn{},
		&longBytesColumn{},
	}

	types := []ColumnType{
		Int8,
		Uint8,
		Int16,
		Uint16,
		Int32,
		Uint32,
		Int64,
		Uint64,
		ShortString,
		ShortBytes,
		LongBytes,
	}

	minimumSizes := []uint64{
		1, 1, 2, 2, 4, 4, 8, 8,
		shortStringMinimumDataByteSize,
		shortBytesMinimumDataByteSize,
		longBytesMinimumDataByteSize,
	}

	maximumSizes := []uint64{
		1, 1, 2, 2, 4, 4, 8, 8,
		shortStringMaximumDataByteSize,
		shortBytesMaximumDataByteSize,
		longBytesMaximumDataByteSize,
	}

	for i, column := range columns {
		if column.Type() != types[i] {
			t.Fatalf("invalid type [%d]%T %v", i, column, types[i])
		}
		if column.MinimumDataByteSize() != minimumSizes[i] {
			t.Fatalf("invalid minimum size [%d]%T %v", i, column, minimumSizes[i])
		}
		if column.MaximumDataByteSize() != maximumSizes[i] {
			t.Fatalf("invalid maximum size [%d]%T %v", i, column, maximumSizes[i])
		}
	}

	t.Skip("TEST IS NOT IMPLEMENTED YET")
}

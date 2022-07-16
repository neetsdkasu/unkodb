// unkodb
// author: Leonardone @ NEETSDKASU

package unkodb

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/neetsdkasu/avltree"
	"github.com/neetsdkasu/avltree/stringkey"
)

func TestColumn(t *testing.T) {

	type byteSizeTestCase struct {
		data     any
		readData any
		byteSize uint64
	}

	type TestCase struct {
		column            Column
		name              string
		columnType        ColumnType
		minimumSize       uint64
		maximumSize       uint64
		validTypeValue    any
		invalidTypeValue  any
		byteSizeTestCases []*byteSizeTestCase
		isKeyColumn       bool
		keyValue          any
		key               avltree.Key
	}

	testCases := []*TestCase{
		&TestCase{
			&intColumn[int8]{name: "foo"},
			"foo",
			Int8,
			1,
			1,
			int8(0x10),
			uint8(0x90),
			[]*byteSizeTestCase{
				&byteSizeTestCase{
					int8(0x12),
					int8(0x12),
					1,
				},
			},
			true,
			int8(1),
			&geneKey[int8]{value: 1},
		},
		&TestCase{
			&intColumn[uint8]{name: "foo"},
			"foo",
			Uint8,
			1,
			1,
			uint8(0x90),
			int8(0x10),
			[]*byteSizeTestCase{
				&byteSizeTestCase{
					uint8(0xFE),
					uint8(0xFE),
					1,
				},
			},
			true,
			uint8(1),
			&geneKey[uint8]{value: 1},
		},
		&TestCase{
			&intColumn[int16]{name: "foo"},
			"foo",
			Int16,
			2,
			2,
			int16(0x1000),
			uint16(0x9000),
			[]*byteSizeTestCase{
				&byteSizeTestCase{
					int16(0x1234),
					int16(0x1234),
					2,
				},
			},
			true,
			int16(1),
			&geneKey[int16]{value: 1},
		},
		&TestCase{
			&intColumn[uint16]{name: "foo"},
			"foo",
			Uint16,
			2,
			2,
			uint16(0x9000),
			int16(0x1000),
			[]*byteSizeTestCase{
				&byteSizeTestCase{
					uint16(0xFEDC),
					uint16(0xFEDC),
					2,
				},
			},
			true,
			uint16(1),
			&geneKey[uint16]{value: 1},
		},
		&TestCase{
			&intColumn[int32]{name: "foo"},
			"foo",
			Int32,
			4,
			4,
			int32(0x1000_0000),
			uint32(0x9000_0000),
			[]*byteSizeTestCase{
				&byteSizeTestCase{
					int32(0x1234_5678),
					int32(0x1234_5678),
					4,
				},
			},
			true,
			int32(1),
			&geneKey[int32]{value: 1},
		},
		&TestCase{
			&intColumn[uint32]{name: "foo"},
			"foo",
			Uint32,
			4,
			4,
			uint32(0x9000_0000),
			int32(0x1000_0000),
			[]*byteSizeTestCase{
				&byteSizeTestCase{
					uint32(0xFEDC_BA98),
					uint32(0xFEDC_BA98),
					4,
				},
			},
			true,
			uint32(1),
			&geneKey[uint32]{value: 1},
		},
		&TestCase{
			&intColumn[int64]{name: "foo"},
			"foo",
			Int64,
			8,
			8,
			int64(0x1000_0000_0000_0000),
			uint64(0x9000_0000_0000_0000),
			[]*byteSizeTestCase{
				&byteSizeTestCase{
					int64(0x1234_5678_9ABC_DEF0),
					int64(0x1234_5678_9ABC_DEF0),
					8,
				},
			},
			true,
			int64(1),
			&geneKey[int64]{value: 1},
		},
		&TestCase{
			&intColumn[uint64]{name: "foo"},
			"foo",
			Uint64,
			8,
			8,
			uint64(0x9000_0000_0000_0000),
			int64(0x1000_0000_0000_0000),
			[]*byteSizeTestCase{
				&byteSizeTestCase{
					uint64(0xFEDC_BA98_7654_3210),
					uint64(0xFEDC_BA98_7654_3210),
					8,
				},
			},
			true,
			uint64(1),
			&geneKey[uint64]{value: 1},
		},
		&TestCase{
			&counterColumn{name: "foo"},
			"foo",
			Counter,
			4,
			4,
			uint32(0x9000_0000),
			int64(0x1000_0000_0000_0000),
			[]*byteSizeTestCase{
				&byteSizeTestCase{
					uint32(0xFEDC_BA98),
					uint32(0xFEDC_BA98),
					4,
				},
			},
			true,
			uint32(1),
			&geneKey[uint32]{value: 1},
		},
		&TestCase{
			&floatColumn[float32]{name: "foo"},
			"foo",
			Float32,
			4,
			4,
			float32(1.234),
			float64(1.234),
			[]*byteSizeTestCase{
				&byteSizeTestCase{
					float32(1.234),
					float32(1.234),
					4,
				},
			},
			false,
			nil,
			nil,
		},
		&TestCase{
			&floatColumn[float64]{name: "foo"},
			"foo",
			Float64,
			8,
			8,
			float64(1.234),
			float32(1.234),
			[]*byteSizeTestCase{
				&byteSizeTestCase{
					float64(1.234),
					float64(1.234),
					8,
				},
			},
			false,
			nil,
			nil,
		},
		&TestCase{
			&shortStringColumn{name: "foo"},
			"foo",
			ShortString,
			0,
			(1 << 8) - 1,
			"abc",
			[]byte("abc"),
			[]*byteSizeTestCase{
				&byteSizeTestCase{
					"abc",
					"abc",
					3 + 1,
				},
			},
			true,
			"abc",
			stringkey.StringKey("abc"),
		},
		&TestCase{
			&fixedSizeShortStringColumn{name: "foo", size: 5},
			"foo",
			FixedSizeShortString,
			5,
			5,
			"abc",
			[]byte("abc"),
			[]*byteSizeTestCase{
				&byteSizeTestCase{
					"abc",
					"abc  ",
					5,
				},
			},
			true,
			"abc",
			stringkey.StringKey("abc  "),
		},
		&TestCase{
			&longStringColumn{name: "foo"},
			"foo",
			LongString,
			0,
			(1 << 16) - 1,
			"abc",
			[]byte("abc"),
			[]*byteSizeTestCase{
				&byteSizeTestCase{
					"abc",
					"abc",
					3 + 2,
				},
			},
			false,
			nil,
			nil,
		},
		&TestCase{
			&fixedSizeLongStringColumn{name: "foo", size: 5},
			"foo",
			FixedSizeLongString,
			5,
			5,
			"abc",
			[]byte("abc"),
			[]*byteSizeTestCase{
				&byteSizeTestCase{
					"abc",
					"abc  ",
					5,
				},
			},
			false,
			nil,
			nil,
		},
		&TestCase{
			&textColumn{name: "foo"},
			"foo",
			Text,
			0,
			(1 << 30) - 1,
			"abc",
			[]byte("abc"),
			[]*byteSizeTestCase{
				&byteSizeTestCase{
					"abc",
					"abc",
					3 + 4,
				},
			},
			false,
			nil,
			nil,
		},
		&TestCase{
			&shortBytesColumn{name: "foo"},
			"foo",
			ShortBytes,
			0,
			(1 << 8) - 1,
			[]byte("abc"),
			"abc",
			[]*byteSizeTestCase{
				&byteSizeTestCase{
					[]byte("abc"),
					[]byte("abc"),
					3 + 1,
				},
			},
			true,
			[]byte("abc"),
			bytesKey([]byte("abc")),
		},
		&TestCase{
			&fixedSizeShortBytesColumn{name: "foo", size: 5},
			"foo",
			FixedSizeShortBytes,
			5,
			5,
			[]byte("abc"),
			"abc",
			[]*byteSizeTestCase{
				&byteSizeTestCase{
					[]byte("abc"),
					append([]byte("abc"), 0, 0),
					5,
				},
			},
			true,
			[]byte("abc"),
			bytesKey(append([]byte("abc"), 0, 0)),
		},
		&TestCase{
			&longBytesColumn{name: "foo"},
			"foo",
			LongBytes,
			0,
			(1 << 16) - 1,
			[]byte("abc"),
			"abc",
			[]*byteSizeTestCase{
				&byteSizeTestCase{
					[]byte("abc"),
					[]byte("abc"),
					3 + 2,
				},
			},
			false,
			nil,
			nil,
		},
		&TestCase{
			&fixedSizeLongBytesColumn{name: "foo", size: 5},
			"foo",
			FixedSizeLongBytes,
			5,
			5,
			[]byte("abc"),
			"abc",
			[]*byteSizeTestCase{
				&byteSizeTestCase{
					[]byte("abc"),
					append([]byte("abc"), 0, 0),
					5,
				},
			},
			false,
			nil,
			nil,
		},
		&TestCase{
			&blobColumn{name: "foo"},
			"foo",
			Blob,
			0,
			(1 << 30) - 1,
			[]byte("abc"),
			"abc",
			[]*byteSizeTestCase{
				&byteSizeTestCase{
					[]byte("abc"),
					[]byte("abc"),
					3 + 4,
				},
			},
			false,
			nil,
			nil,
		},
	}

	for i, tc := range testCases {
		if tc.column.Name() != tc.name {
			t.Fatalf("wrong name [%d] %#v", i, tc)
		}

		if tc.column.Type() != tc.columnType {
			t.Fatalf("wrong type [%d] %#v", i, tc)
		}

		if tc.column.MinimumDataByteSize() != tc.minimumSize {
			t.Fatalf("wrong minimumSize [%d] %#v", i, tc)
		}

		if tc.column.MaximumDataByteSize() != tc.maximumSize {
			t.Fatalf("wrong maximumSize [%d] %#v", i, tc)
		}

		if !tc.column.IsValidValueType(tc.validTypeValue) {
			t.Fatalf("wrong valid value type [%d] %#v", i, tc)
		}

		if tc.column.IsValidValueType(tc.invalidTypeValue) {
			t.Fatalf("wrong invalid value type [%d] %#v", i, tc)
		}

		for k, bsh := range tc.byteSizeTestCases {
			size := tc.column.byteSizeHint(bsh.data)
			if size != bsh.byteSize {
				t.Fatalf("wrong byteSizeHint [%d,%d] %#v %#v", i, k, tc, size)
			}

			var b bytes.Buffer
			w := newByteEncoder(&b, fileByteOrder)
			err := tc.column.write(w, bsh.data)
			if err != nil {
				t.Fatalf("wrong write [%d,%d] %#v %#v", i, k, tc, err)
			}
			if b.Len() != int(bsh.byteSize) {
				t.Fatalf("wrong write byte size [%d,%d] %#v %#v", i, k, tc, b.Bytes())
			}

			r := newByteDecoder(bytes.NewReader(b.Bytes()), fileByteOrder)
			data, err := tc.column.read(r)
			if err != nil {
				t.Fatalf("wrong read [%d,%d] %#v %#v", i, k, tc, err)
			}
			if !reflect.DeepEqual(bsh.readData, data) {
				t.Fatalf("wrong read data [%d,%d] %#v %#v", i, k, tc, data)
			}
		}

		if kc, ok := tc.column.(keyColumn); ok != tc.isKeyColumn {
			t.Fatalf("wrong keyColumn implement [%d] %#v", i, tc)
		} else if ok {
			key := kc.toKey(tc.keyValue)
			if !reflect.DeepEqual(key, tc.key) {
				t.Fatalf("wrong key [%d] %#v %#v", i, tc, key)
			}
		}
	}
}

// unkodb
// author: Leonardone @ NEETSDKASU

package unkodb

import (
	"testing"
)

func TestTableCreator(t *testing.T) {

	{
		tc := newTableCreator(nil, "")
		tc.Int8Key("foo")
		if c, ok := tc.key.(*intColumn[int8]); !ok {
			t.Fatal("bad")
		} else if c.Name() != "foo" {
			t.Fatal("bad")
		}
	}

	{
		tc := newTableCreator(nil, "")
		tc.Uint8Key("foo")
		if c, ok := tc.key.(*intColumn[uint8]); !ok {
			t.Fatal("bad")
		} else if c.Name() != "foo" {
			t.Fatal("bad")
		}
	}

	{
		tc := newTableCreator(nil, "")
		tc.Int16Key("foo")
		if c, ok := tc.key.(*intColumn[int16]); !ok {
			t.Fatal("bad")
		} else if c.Name() != "foo" {
			t.Fatal("bad")
		}
	}

	{
		tc := newTableCreator(nil, "")
		tc.Uint16Key("foo")
		if c, ok := tc.key.(*intColumn[uint16]); !ok {
			t.Fatal("bad")
		} else if c.Name() != "foo" {
			t.Fatal("bad")
		}
	}

	{
		tc := newTableCreator(nil, "")
		tc.Int32Key("foo")
		if c, ok := tc.key.(*intColumn[int32]); !ok {
			t.Fatal("bad")
		} else if c.Name() != "foo" {
			t.Fatal("bad")
		}
	}

	{
		tc := newTableCreator(nil, "")
		tc.Uint32Key("foo")
		if c, ok := tc.key.(*intColumn[uint32]); !ok {
			t.Fatal("bad")
		} else if c.Name() != "foo" {
			t.Fatal("bad")
		}
	}

	{
		tc := newTableCreator(nil, "")
		tc.Int64Key("foo")
		if c, ok := tc.key.(*intColumn[int64]); !ok {
			t.Fatal("bad")
		} else if c.Name() != "foo" {
			t.Fatal("bad")
		}
	}

	{
		tc := newTableCreator(nil, "")
		tc.Uint64Key("foo")
		if c, ok := tc.key.(*intColumn[uint64]); !ok {
			t.Fatal("bad")
		} else if c.Name() != "foo" {
			t.Fatal("bad")
		}
	}

	{
		tc := newTableCreator(nil, "")
		tc.Int8Column("foo")
		if c, ok := tc.columns[0].(*intColumn[int8]); !ok {
			t.Fatal("bad")
		} else if c.Name() != "foo" {
			t.Fatal("bad")
		}
	}

	{
		tc := newTableCreator(nil, "")
		tc.Uint8Column("foo")
		if c, ok := tc.columns[0].(*intColumn[uint8]); !ok {
			t.Fatal("bad")
		} else if c.Name() != "foo" {
			t.Fatal("bad")
		}
	}

	{
		tc := newTableCreator(nil, "")
		tc.Int16Column("foo")
		if c, ok := tc.columns[0].(*intColumn[int16]); !ok {
			t.Fatal("bad")
		} else if c.Name() != "foo" {
			t.Fatal("bad")
		}
	}

	{
		tc := newTableCreator(nil, "")
		tc.Uint16Column("foo")
		if c, ok := tc.columns[0].(*intColumn[uint16]); !ok {
			t.Fatal("bad")
		} else if c.Name() != "foo" {
			t.Fatal("bad")
		}
	}

	{
		tc := newTableCreator(nil, "")
		tc.Int32Column("foo")
		if c, ok := tc.columns[0].(*intColumn[int32]); !ok {
			t.Fatal("bad")
		} else if c.Name() != "foo" {
			t.Fatal("bad")
		}
	}

	{
		tc := newTableCreator(nil, "")
		tc.Uint32Column("foo")
		if c, ok := tc.columns[0].(*intColumn[uint32]); !ok {
			t.Fatal("bad")
		} else if c.Name() != "foo" {
			t.Fatal("bad")
		}
	}

	{
		tc := newTableCreator(nil, "")
		tc.Int64Column("foo")
		if c, ok := tc.columns[0].(*intColumn[int64]); !ok {
			t.Fatal("bad")
		} else if c.Name() != "foo" {
			t.Fatal("bad")
		}
	}

	{
		tc := newTableCreator(nil, "")
		tc.Uint64Column("foo")
		if c, ok := tc.columns[0].(*intColumn[uint64]); !ok {
			t.Fatal("bad")
		} else if c.Name() != "foo" {
			t.Fatal("bad")
		}
	}

	{
		tc := newTableCreator(nil, "")
		tc.CounterKey("foo")
		if c, ok := tc.key.(*counterColumn); !ok {
			t.Fatal("bad")
		} else if c.Name() != "foo" {
			t.Fatal("bad")
		}
	}

	{
		tc := newTableCreator(nil, "")
		tc.Float32Column("foo")
		if c, ok := tc.columns[0].(*floatColumn[float32]); !ok {
			t.Fatal("bad")
		} else if c.Name() != "foo" {
			t.Fatal("bad")
		}
	}

	{
		tc := newTableCreator(nil, "")
		tc.Float64Column("foo")
		if c, ok := tc.columns[0].(*floatColumn[float64]); !ok {
			t.Fatal("bad")
		} else if c.Name() != "foo" {
			t.Fatal("bad")
		}
	}

	{
		tc := newTableCreator(nil, "")
		tc.ShortStringKey("foo")
		if c, ok := tc.key.(*shortStringColumn); !ok {
			t.Fatal("bad")
		} else if c.Name() != "foo" {
			t.Fatal("bad")
		}
	}

	{
		tc := newTableCreator(nil, "")
		tc.FixedSizeShortStringKey("foo", 100)
		if c, ok := tc.key.(*fixedSizeShortStringColumn); !ok {
			t.Fatal("bad")
		} else if c.Name() != "foo" || c.byteSizeHint("") != 100 {
			t.Fatal("bad")
		}
	}

	{
		tc := newTableCreator(nil, "")
		tc.ShortStringColumn("foo")
		if c, ok := tc.columns[0].(*shortStringColumn); !ok {
			t.Fatal("bad")
		} else if c.Name() != "foo" {
			t.Fatal("bad")
		}
	}

	{
		tc := newTableCreator(nil, "")
		tc.FixedSizeShortStringColumn("foo", 100)
		if c, ok := tc.columns[0].(*fixedSizeShortStringColumn); !ok {
			t.Fatal("bad")
		} else if c.Name() != "foo" || c.byteSizeHint("") != 100 {
			t.Fatal("bad")
		}
	}

	{
		tc := newTableCreator(nil, "")
		tc.LongStringColumn("foo")
		if c, ok := tc.columns[0].(*longStringColumn); !ok {
			t.Fatal("bad")
		} else if c.Name() != "foo" {
			t.Fatal("bad")
		}
	}

	{
		tc := newTableCreator(nil, "")
		tc.FixedSizeLongStringColumn("foo", 1000)
		if c, ok := tc.columns[0].(*fixedSizeLongStringColumn); !ok {
			t.Fatal("bad")
		} else if c.Name() != "foo" || c.byteSizeHint("") != 1000 {
			t.Fatal("bad")
		}
	}

	{
		tc := newTableCreator(nil, "")
		tc.TextColumn("foo")
		if c, ok := tc.columns[0].(*textColumn); !ok {
			t.Fatal("bad")
		} else if c.Name() != "foo" {
			t.Fatal("bad")
		}
	}

	{
		tc := newTableCreator(nil, "")
		tc.ShortBytesKey("foo")
		if c, ok := tc.key.(*shortBytesColumn); !ok {
			t.Fatal("bad")
		} else if c.Name() != "foo" {
			t.Fatal("bad")
		}
	}

	{
		tc := newTableCreator(nil, "")
		tc.FixedSizeShortBytesKey("foo", 100)
		if c, ok := tc.key.(*fixedSizeShortBytesColumn); !ok {
			t.Fatal("bad")
		} else if c.Name() != "foo" || c.byteSizeHint([]byte{}) != 100 {
			t.Fatal("bad")
		}
	}

	{
		tc := newTableCreator(nil, "")
		tc.ShortBytesColumn("foo")
		if c, ok := tc.columns[0].(*shortBytesColumn); !ok {
			t.Fatal("bad")
		} else if c.Name() != "foo" {
			t.Fatal("bad")
		}
	}

	{
		tc := newTableCreator(nil, "")
		tc.FixedSizeShortBytesColumn("foo", 100)
		if c, ok := tc.columns[0].(*fixedSizeShortBytesColumn); !ok {
			t.Fatal("bad")
		} else if c.Name() != "foo" || c.byteSizeHint([]byte{}) != 100 {
			t.Fatal("bad")
		}
	}

	{
		tc := newTableCreator(nil, "")
		tc.LongBytesColumn("foo")
		if c, ok := tc.columns[0].(*longBytesColumn); !ok {
			t.Fatal("bad")
		} else if c.Name() != "foo" {
			t.Fatal("bad")
		}
	}

	{
		tc := newTableCreator(nil, "")
		tc.FixedSizeLongBytesColumn("foo", 1000)
		if c, ok := tc.columns[0].(*fixedSizeLongBytesColumn); !ok {
			t.Fatal("bad")
		} else if c.Name() != "foo" || c.byteSizeHint([]byte{}) != 1000 {
			t.Fatal("bad")
		}
	}

	{
		tc := newTableCreator(nil, "")
		tc.BlobColumn("foo")
		if c, ok := tc.columns[0].(*blobColumn); !ok {
			t.Fatal("bad")
		} else if c.Name() != "foo" {
			t.Fatal("bad")
		}
	}

	t.Skip("TEST IS NOT IMPLEMENTED YET")
}

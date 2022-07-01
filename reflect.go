// unkodb
// author: Leonardone @ NEETSDKASU

package unkodb

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

var simpleColumnTypes = make(map[string]ColumnType)

func init() {
	cts := []ColumnType{
		Counter,
		Int8,
		Uint8,
		Int16,
		Uint16,
		Int32,
		Uint32,
		Int64,
		Uint64,
		Float32,
		Float64,
		ShortString,
		LongString,
		Text,
		ShortBytes,
		LongBytes,
		Blob,
	}
	for _, ct := range cts {
		simpleColumnTypes[ct.String()] = ct
	}
}

func parseTagColumnType(s string) (isKey bool, ct ColumnType, size uint64, err error) {
	if strings.HasPrefix(s, "key@") {
		isKey = true
		s = strings.TrimPrefix(s, "key@")
	}
	if tmp, ok := simpleColumnTypes[s]; ok {
		if isKey && !tmp.keyColumnType() {
			err = fmt.Errorf("invalid key type")
		} else {
			ct = tmp
		}
		return
	}
	type ft struct {
		ct  ColumnType
		max uint64
	}
	var fts = [4]ft{
		ft{FixedSizeShortString, shortStringMaximumDataByteSize},
		ft{FixedSizeLongString, longStringMaximumDataByteSize},
		ft{FixedSizeShortBytes, shortBytesMaximumDataByteSize},
		ft{FixedSizeLongBytes, longBytesMaximumDataByteSize},
	}
	ct = invalidColumnType
	for _, x := range fts {
		p := x.ct.String()
		if strings.HasPrefix(s, p) {
			s = strings.TrimPrefix(s, p)
			ct = x.ct
			size = x.max
			break
		}
	}
	if ct == invalidColumnType {
		err = fmt.Errorf("not type name")
		return
	}
	if !strings.HasPrefix(s, "(") || !strings.HasSuffix(s, ")") {
		err = fmt.Errorf("tag syntax error")
		return
	}
	s = strings.TrimSuffix(strings.TrimPrefix(s, "("), ")")
	tmp, e := strconv.ParseUint(s, 10, 16)
	if e != nil || tmp == 0 || tmp > size {
		err = fmt.Errorf("wrong size")
		return
	}
	size = tmp
	return
}

func tryConvertValue(v reflect.Value, ct ColumnType, size uint64) (r reflect.Value, ok bool) {
	for v.Kind() == reflect.Pointer {
		v = v.Elem()
	}
	switch ct {
	case Counter:
		if v.Kind() == reflect.Uint32 {
			r = v
			ok = true
		} else if v.CanConvert(reflect.TypeOf(uint32(0))) {
			r = v.Convert(reflect.TypeOf(uint32(0)))
			ok = true
		}
	case Int8:
		// TODO
	case Uint8:
		// TODO
	case Int16:
		// TODO
	case Uint16:
		// TODO
	case Int32:
		// TODO
	case Uint32:
		// TODO
	case Int64:
		// TODO
	case Uint64:
		// TODO
	case Float32:
		// TODO
	case Float64:
		// TODO
	case ShortString:
		// TODO
	case FixedSizeShortString:
		// TODO
	case LongString:
		// TODO
	case FixedSizeLongString:
		// TODO
	case Text:
		// TODO
	case ShortBytes:
		// TODO
	case FixedSizeShortBytes:
		// TODO
	case LongBytes:
		// TODO
	case FixedSizeLongBytes:
		// TODO
	case Blob:
		// TODO
	}
	return
}

func parseStruct(st any) (map[string]any, error) {
	v := reflect.ValueOf(st)
	for v.Kind() == reflect.Pointer {
		// forじゃなくifがいいのだろうか・・・・？
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return nil, notStruct
	}
	hasKey := false
	m := make(map[string]any)
	for _, f := range reflect.VisibleFields(v.Type()) {
		tv, ok := f.Tag.Lookup("unkodb")
		if !ok {
			continue
		}
		value := v.FieldByIndex(f.Index)
		index := strings.LastIndex(tv, ",")
		mKey := tv
		if index >= 0 {
			isKey, ct, size, e := parseTagColumnType(tv[index+1:])
			if e == nil {
				if isKey {
					if hasKey {
						return nil, fmt.Errorf("duplicate key")
					}
					hasKey = true
				}
				mKey = tv[:index]
				value, ok = tryConvertValue(value, ct, size)
				if !ok {
					return nil, fmt.Errorf("cannot convert type")
				}
			}
		}
		if len(mKey) == 0 {
			mKey = f.Name
		}
		if _, ok = m[mKey]; ok {
			return nil, fmt.Errorf("duplicate name")
		}
		m[mKey] = value.Interface()
	}
	if !hasKey {
		return nil, fmt.Errorf("not found key tag")
	}
	return m, nil
}

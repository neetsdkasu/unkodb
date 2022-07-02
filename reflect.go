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

func parseData(data any) (tableTreeValue, error) {
	if data == nil {
		return nil, NotFoundData
	}
	if m, ok := data.(tableTreeValue); ok {
		return m, nil
	}
	if m, err := parseStruct(data); err != notStruct {
		return m, err
	}
	v := reflect.ValueOf(data)
	for v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return nil, NotFoundData
		}
		v = v.Elem()
	}
	if v.Kind() != reflect.Map || v.IsNil() {
		return nil, NotFoundData
	}
	keyType := v.Type().Key()
	stringerType := reflect.TypeOf((*fmt.Stringer)(nil)).Elem()
	if keyType.Kind() != reflect.String && !keyType.Implements(stringerType) {
		return nil, NotFoundData
	}
	r := make(tableTreeValue)
	iter := v.MapRange()
	for iter.Next() {
		key := iter.Key().Interface()
		value := iter.Value().Interface()
		if keyType.Kind() == reflect.String {
			r[key.(string)] = value
		} else {
			r[key.(fmt.Stringer).String()] = value
		}
	}
	return r, nil
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
		err = fmt.Errorf("not found type name")
		return
	}
	if isKey && !ct.keyColumnType() {
		err = fmt.Errorf("invalid key type")
		return
	}
	if !strings.HasPrefix(s, "(") || !strings.HasSuffix(s, ")") {
		err = fmt.Errorf("not found size syntax")
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
	case Counter, Uint32:
		if v.Kind() == reflect.Uint32 {
			r = v
			ok = true
		} else if v.CanConvert(reflect.TypeOf(uint32(0))) {
			// 精度を下げるキャストが発生する･･･？
			r = v.Convert(reflect.TypeOf(uint32(0)))
			ok = true
		}
	case Int8:
		if v.Kind() == reflect.Int8 {
			r = v
			ok = true
		} else if v.CanConvert(reflect.TypeOf(int8(0))) {
			// 精度を下げるキャストが発生する･･･？
			r = v.Convert(reflect.TypeOf(int8(0)))
			ok = true
		}
	case Uint8:
		if v.Kind() == reflect.Uint8 {
			r = v
			ok = true
		} else if v.CanConvert(reflect.TypeOf(uint8(0))) {
			// 精度を下げるキャストが発生する･･･？
			r = v.Convert(reflect.TypeOf(uint8(0)))
			ok = true
		}
	case Int16:
		if v.Kind() == reflect.Int16 {
			r = v
			ok = true
		} else if v.CanConvert(reflect.TypeOf(int16(0))) {
			// 精度を下げるキャストが発生する･･･？
			r = v.Convert(reflect.TypeOf(int16(0)))
			ok = true
		}
	case Uint16:
		if v.Kind() == reflect.Uint16 {
			r = v
			ok = true
		} else if v.CanConvert(reflect.TypeOf(uint16(0))) {
			// 精度を下げるキャストが発生する･･･？
			r = v.Convert(reflect.TypeOf(uint16(0)))
			ok = true
		}
	case Int32:
		if v.Kind() == reflect.Int32 {
			r = v
			ok = true
		} else if v.CanConvert(reflect.TypeOf(int32(0))) {
			// 精度を下げるキャストが発生する･･･？
			r = v.Convert(reflect.TypeOf(int32(0)))
			ok = true
		}
	case Int64:
		if v.Kind() == reflect.Int64 {
			r = v
			ok = true
		} else if v.CanConvert(reflect.TypeOf(int64(0))) {
			// 精度を下げるキャストが発生する･･･？
			r = v.Convert(reflect.TypeOf(int64(0)))
			ok = true
		}
	case Uint64:
		if v.Kind() == reflect.Uint64 {
			r = v
			ok = true
		} else if v.CanConvert(reflect.TypeOf(uint64(0))) {
			// 精度を下げるキャストが発生する･･･？
			r = v.Convert(reflect.TypeOf(uint64(0)))
			ok = true
		}
	case Float32:
		if v.Kind() == reflect.Float32 {
			r = v
			ok = true
		} else if v.CanConvert(reflect.TypeOf(float32(0))) {
			// 精度を下げるキャストが発生する･･･？
			r = v.Convert(reflect.TypeOf(float32(0)))
			ok = true
		}
	case Float64:
		if v.Kind() == reflect.Float64 {
			r = v
			ok = true
		} else if v.CanConvert(reflect.TypeOf(float64(0))) {
			// 精度を下げるキャストが発生する･･･？
			r = v.Convert(reflect.TypeOf(float64(0)))
			ok = true
		}
	case ShortString, FixedSizeShortString, LongString, FixedSizeLongString, Text:
		if v.Kind() == reflect.String {
			r = v
			ok = true
		}
	case ShortBytes, FixedSizeShortBytes, LongBytes, FixedSizeLongBytes, Blob:
		// スライスの長さを適性に変更するまではしなくていいか･･･？
		if v.Kind() == reflect.Slice {
			if v.Type().Elem().Kind() == reflect.Uint8 {
				r = v
				ok = true
			}
		} else if v.Kind() == reflect.Array {
			if v.Type().Elem().Kind() == reflect.Uint8 {
				sl := v.Len()
				r = v.Slice(0, sl)
				ok = true
			}
		}
	}
	return
}

func parseStruct(st any) (tableTreeValue, error) {
	v := reflect.ValueOf(st)
	for v.Kind() == reflect.Pointer {
		// forじゃなくifがいいのだろうか・・・・？
		if v.IsNil() {
			return nil, NotFoundData
		}
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return nil, notStruct
	}
	hasKey := false
	m := make(tableTreeValue)
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
			if e != nil {
				return nil, TagError{fmt.Errorf("%w (field: %s)", e, f.Name)}
			}
			if isKey {
				if hasKey {
					return nil, TagError{fmt.Errorf("duplicate key (field: %s)", f.Name)}
				}
				hasKey = true
			}
			mKey = tv[:index]
			value, ok = tryConvertValue(value, ct, size)
			if !ok {
				return nil, TagError{fmt.Errorf("cannot convert type %s to %s (field: %s)", f.Type, ct.GoTypeHint(), f.Name)}
			}
		}
		if len(mKey) == 0 {
			mKey = f.Name
		}
		if _, ok = m[mKey]; ok {
			return nil, TagError{fmt.Errorf(`duplicate name "%s" (field: %s)`, mKey, f.Name)}
		}
		m[mKey] = value.Interface()
	}
	return m, nil
}

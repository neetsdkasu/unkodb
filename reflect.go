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

func createTableByTag(tc *TableCreator, st any) error {
	t := reflect.TypeOf(st)
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return notStruct
	}
	hasKey := false
	m := make(map[string]bool)
	for _, f := range reflect.VisibleFields(t) {
		tv, ok := f.Tag.Lookup("unkodb")
		if !ok {
			continue
		}
		index := strings.LastIndex(tv, ",")
		mKey := tv
		var (
			isKey bool
			ct    ColumnType
			size  uint64
			err   error
		)
		if index < 0 {
			ct, size, err = inferColumnType(f.Type)
			if err != nil {
				return TagError{fmt.Errorf("%w (field: %s)", err, f.Name)}
			}
		} else {
			mKey = tv[:index]
			isKey, ct, size, err = parseTagColumnType(tv[index+1:])
			if err != nil {
				return TagError{fmt.Errorf("%w (field: %s)", err, f.Name)}
			}
			if isKey {
				if hasKey {
					return TagError{fmt.Errorf("duplicate key (field: %s)", f.Name)}
				}
				hasKey = true
			}
			if !canConvertType(f.Type, ct, size) {
				return TagError{fmt.Errorf("cannot convert type %s to %s (field: %s)", f.Type, ct.GoTypeHint(), f.Name)}
			}
		}
		if len(mKey) == 0 {
			mKey = f.Name
		}
		if _, ok = m[mKey]; ok {
			return TagError{fmt.Errorf(`duplicate name "%s" (field: %s)`, mKey, f.Name)}
		}
		m[mKey] = true
		err = makeColumn(tc, mKey, isKey, ct, size)
		if err != nil {
			return err
		}
	}
	if !hasKey {
		return NotFoundKey
	}
	return nil
}

func makeColumn(tc *TableCreator, mKey string, isKey bool, ct ColumnType, size uint64) (err error) {
	switch ct {
	default:
		bug.Panicf("invalid column type %d", int(ct))
	case Counter:
		if isKey {
			err = tc.CounterKey(mKey)
		} else {
			bug.Panic("UNREACHABLE")
		}
	case Int8:
		if isKey {
			err = tc.Int8Key(mKey)
		} else {
			err = tc.Int8Column(mKey)
		}
	case Uint8:
		if isKey {
			err = tc.Uint8Key(mKey)
		} else {
			err = tc.Uint8Column(mKey)
		}
	case Int16:
		if isKey {
			err = tc.Int16Key(mKey)
		} else {
			err = tc.Int16Column(mKey)
		}
	case Uint16:
		if isKey {
			err = tc.Uint16Key(mKey)
		} else {
			err = tc.Uint16Column(mKey)
		}
	case Int32:
		if isKey {
			err = tc.Int32Key(mKey)
		} else {
			err = tc.Int32Column(mKey)
		}
	case Uint32:
		if isKey {
			err = tc.Uint32Key(mKey)
		} else {
			err = tc.Uint32Column(mKey)
		}
	case Int64:
		if isKey {
			bug.Panic("UNREACHABLE")
		} else {
			err = tc.Int64Column(mKey)
		}
	case Uint64:
		if isKey {
			bug.Panic("UNREACHABLE")
		} else {
			err = tc.Uint64Column(mKey)
		}
	case Float32:
		if isKey {
			bug.Panic("UNREACHABLE")
		} else {
			err = tc.Float32Column(mKey)
		}
	case Float64:
		if isKey {
			bug.Panic("UNREACHABLE")
		} else {
			err = tc.Float64Column(mKey)
		}
	case ShortString:
		if isKey {
			err = tc.ShortStringKey(mKey)
		} else {
			err = tc.ShortStringColumn(mKey)
		}
	case FixedSizeShortString:
		if size == 0 || size > shortStringMaximumDataByteSize {
			err = fmt.Errorf("1 <= FixedSizeShortString size <= %d", shortStringMaximumDataByteSize)
			return
		}
		if isKey {
			err = tc.FixedSizeShortStringKey(mKey, uint8(size))
		} else {
			err = tc.FixedSizeShortStringColumn(mKey, uint8(size))
		}
	case LongString:
		if isKey {
			bug.Panic("UNREACHABLE")
		} else {
			err = tc.LongStringColumn(mKey)
		}
	case FixedSizeLongString:
		if size == 0 || size > longStringMaximumDataByteSize {
			err = fmt.Errorf("1 <= FixedSizeLongString size <= %d", longStringMaximumDataByteSize)
			return
		}
		if isKey {
			bug.Panic("UNREACHABLE")
		} else {
			err = tc.FixedSizeLongStringColumn(mKey, uint16(size))
		}
	case Text:
		if isKey {
			bug.Panic("UNREACHABLE")
		} else {
			err = tc.TextColumn(mKey)
		}
	case ShortBytes:
		if isKey {
			err = tc.ShortBytesKey(mKey)
		} else {
			err = tc.ShortBytesColumn(mKey)
		}
	case FixedSizeShortBytes:
		if size == 0 || size > shortBytesMaximumDataByteSize {
			err = fmt.Errorf("1 <= FixedSizeShortBytes size <= %d", shortBytesMaximumDataByteSize)
			return
		}
		if isKey {
			err = tc.FixedSizeShortBytesKey(mKey, uint8(size))
		} else {
			err = tc.FixedSizeShortBytesColumn(mKey, uint8(size))
		}
	case LongBytes:
		if isKey {
			bug.Panic("UNREACHABLE")
		} else {
			err = tc.LongBytesColumn(mKey)
		}
	case FixedSizeLongBytes:
		if size == 0 || size > longBytesMaximumDataByteSize {
			err = fmt.Errorf("1 <= FixedSizeLongBytes size <= %d", longBytesMaximumDataByteSize)
			return
		}
		if isKey {
			bug.Panic("UNREACHABLE")
		} else {
			err = tc.FixedSizeLongBytesColumn(mKey, uint16(size))
		}
	case Blob:
		if isKey {
			bug.Panic("UNREACHABLE")
		} else {
			err = tc.BlobColumn(mKey)
		}
	}
	return
}

func inferColumnType(t reflect.Type) (ct ColumnType, size uint64, err error) {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	switch t.Kind() {
	default:
		err = fmt.Errorf("cannot convert to column type")
	case reflect.Int8:
		ct = Int8
	case reflect.Int16:
		ct = Int16
	case reflect.Int32:
		ct = Int32
	case reflect.Int64:
		ct = Int64
	case reflect.Uint8:
		ct = Uint8
	case reflect.Uint16:
		ct = Uint16
	case reflect.Uint32:
		ct = Uint32
	case reflect.Uint64:
		ct = Uint64
	case reflect.Float32:
		ct = Float32
	case reflect.Float64:
		ct = Float64
	case reflect.String:
		ct = Text
	case reflect.Slice:
		if t.Elem().Kind() == reflect.Uint8 {
			ct = Blob
		} else {
			err = fmt.Errorf("cannot convert to columun type")
		}
	case reflect.Array:
		if t.Elem().Kind() == reflect.Uint8 {
			if t.Len() == 0 {
				err = fmt.Errorf("cannot convert to columun type")
			} else if t.Len() <= shortStringMaximumDataByteSize {
				ct = FixedSizeShortBytes
				size = uint64(t.Len())
			} else if t.Len() <= longBytesMaximumDataByteSize {
				ct = FixedSizeLongBytes
				size = uint64(t.Len())
			} else {
				ct = Blob
			}
		} else {
			err = fmt.Errorf("cannot convert to columun type")
		}
	}
	return
}

func canConvertType(t reflect.Type, ct ColumnType, size uint64) bool {
	panic("TODO")
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
		if tmp == Counter && !isKey {
			err = fmt.Errorf(`Counter type need prefix "key@"`)
			return
		}
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
	if !strings.HasPrefix(s, "[") || !strings.HasSuffix(s, "]") {
		err = fmt.Errorf("not found size syntax")
		return
	}
	s = strings.TrimSuffix(strings.TrimPrefix(s, "["), "]")
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
		if v.IsNil() {
			return
		}
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
		for value.Kind() == reflect.Pointer {
			if value.IsNil() {
				return nil, NotFoundData
			}
			value = value.Elem()
		}
		index := strings.LastIndex(tv, ",")
		mKey := tv
		if index < 0 {
			if value.Kind() == reflect.Array {
				sl := value.Len()
				value = value.Slice(0, sl)
			}
		} else {
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

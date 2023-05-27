// unkodb
// author: Leonardone @ NEETSDKASU

package unkodb

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

// テーブルとデータをやりとりする際に使うことができる簡易データホルダー。
// キーやカラムのカラム型に対応したGoの型で値を設定する必要がある。
// Columnsの値はテーブルのColumnsと同じ順番で設定する必要がある。
//
//	tc, _ := db.CreateTable("my_book_table")
//	tc.CounterKey("id")
//	tc.ShortStringColumn("title")
//	tc.ShortStringColumn("author")
//	tc.Int64Column("price")
//	table, _ := tc.Create()
//	table.Insert(unkodb.Data{
//		Key: unkodb.CounterType(0),
//		Columns: []any{
//			"プログラミング入門",
//			"いにしえのプログラマー",
//			int64(4800),
//		},
//	})
type Data struct {
	Key     any
	Columns []any
}

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

func moveData(r *Record, dst any) (err error) {
	if dst == nil {
		// TODO 適切なエラーに直す
		err = ErrNotFoundData
		return
	}
	if m, ok := dst.(tableTreeValue); ok {
		if m == nil {
			// TODO 適切なエラーに直す
			err = ErrNotFoundData
		} else {
			for name, value := range r.data {
				m[name] = value
			}
		}
		return
	}
	if ap, ok := dst.(*any); ok {
		if ap == nil {
			// TODO 適切なエラーに直す
			err = ErrNotFoundData
		} else if *ap == nil {
			*ap = r.data
		} else {
			err = moveData(r, *ap)
		}
		return
	}
	if err = moveDataToDataStruct(r, dst); err != errNotStruct {
		return
	}
	if err = moveDataToTaggedStruct(r, dst); err != errNotStruct {
		return
	}
	v := reflect.ValueOf(dst)
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			// TODO 適切なエラーに直す
			err = ErrNotFoundData
		} else {
			err = moveData(r, v.Elem().Interface())
		}
		return
	}
	if v.Kind() != reflect.Map || v.IsNil() || !v.CanSet() {
		// TODO 適切なエラーに直す
		err = ErrNotFoundData
		return
	}
	if v.Type().Key() != reflect.TypeOf("") {
		// TODO 適切なエラーに直す
		err = ErrNotFoundData
		return
	}
	et := v.Type().Elem()
	for _, value := range r.data {
		if !reflect.ValueOf(value).CanConvert(et) {
			// TODO 適切なエラーに直す
			err = ErrNotFoundData
			return
		}
	}
	for name, value := range r.data {
		cv := reflect.ValueOf(value).Convert(et)
		v.SetMapIndex(reflect.ValueOf(name), cv)
	}
	err = nil
	return
}

func fillData(r *Record, dst any) (err error) {
	if dst == nil {
		// TODO 適切なエラーに直す
		err = ErrNotFoundData
		return
	}
	if m, ok := dst.(tableTreeValue); ok {
		if m == nil {
			// TODO 適切なエラーに直す
			err = ErrNotFoundData
		} else {
			m[r.table.key.Name()] = r.table.key.copyValue(r.Key())
			for _, col := range r.table.columns {
				m[col.Name()] = col.copyValue(r.Column(col.Name()))
			}
		}
		return
	}
	if ap, ok := dst.(*any); ok {
		if ap == nil {
			// TODO 適切なエラーに直す
			err = ErrNotFoundData
		} else {
			if *ap == nil {
				*ap = make(tableTreeValue)
			}
			err = fillData(r, *ap)
		}
		return
	}
	if err = fillDataToDataStruct(r, dst); err != errNotStruct {
		return
	}
	if err = fillDataToTaggedStruct(r, dst); err != errNotStruct {
		return
	}
	v := reflect.ValueOf(dst)
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			// TODO 適切なエラーに直す
			err = ErrNotFoundData
		} else {
			err = fillData(r, v.Elem().Interface())
		}
		return
	}
	if v.Kind() != reflect.Map || v.IsNil() || !v.CanSet() {
		// TODO 適切なエラーに直す
		err = ErrNotFoundData
		return
	}
	if v.Type().Key() != reflect.TypeOf("") {
		// TODO 適切なエラーに直す
		err = ErrNotFoundData
		return
	}
	et := v.Type().Elem()
	for _, value := range r.data {
		if !reflect.ValueOf(value).CanConvert(et) {
			// TODO 適切なエラーに直す
			err = ErrNotFoundData
			return
		}
	}
	for name, value := range r.data {
		value = r.table.Column(name).copyValue(value)
		cv := reflect.ValueOf(value).Convert(et)
		v.SetMapIndex(reflect.ValueOf(name), cv)
	}
	err = nil
	return
}

func moveDataToDataStruct(r *Record, st any) error {
	if st == nil {
		return errNotStruct
	}
	v := reflect.ValueOf(st)
	if v.Kind() != reflect.Pointer {
		return errNotStruct
	}
	dt := reflect.TypeOf((*Data)(nil))
	for v.Kind() == reflect.Pointer {
		if v.Type() == dt {
			break
		}
		if v.IsNil() {
			return errNotStruct
		}
		v = v.Elem()
	}
	if v.Kind() != reflect.Pointer {
		return errNotStruct
	}
	if v.IsNil() {
		if !v.CanSet() {
			return errNotStruct
		}
		v.Set(reflect.ValueOf(new(Data)))
	}
	// fillDataToTaggedStruct/tryFillDataValueでは
	// byteスライスが存在する場合は再利用するのに
	// こちらでは別にコピーを生成して代入して、元のスライスを破棄してしまってる
	// 例えば
	//  copy(d.Columns[0].([]byte), r.Columns[0])
	// などとした場合、サイズが合わなければ結局割り当てなおしが必要なわけで…
	// 普通に最初からコピーの割り当てでもいいか
	d := v.Interface().(*Data)
	d.Key = r.Key()
	if d.Columns != nil {
		d.Columns = d.Columns[:0]
	}
	for _, col := range r.table.columns {
		d.Columns = append(d.Columns, r.Column(col.Name()))
	}
	return nil
}

func fillDataToDataStruct(r *Record, st any) error {
	if st == nil {
		return errNotStruct
	}
	v := reflect.ValueOf(st)
	if v.Kind() != reflect.Pointer {
		return errNotStruct
	}
	dt := reflect.TypeOf((*Data)(nil))
	for v.Kind() == reflect.Pointer {
		if v.Type() == dt {
			break
		}
		if v.IsNil() {
			return errNotStruct
		}
		v = v.Elem()
	}
	if v.Kind() != reflect.Pointer {
		return errNotStruct
	}
	if v.IsNil() {
		if !v.CanSet() {
			return errNotStruct
		}
		v.Set(reflect.ValueOf(new(Data)))
	}
	// fillDataToTaggedStruct/tryFillDataValueでは
	// byteスライスが存在する場合は再利用するのに
	// こちらでは別にコピーを生成して代入して、元のスライスを破棄してしまってる
	// 例えば
	//  copy(d.Columns[0].([]byte), r.Columns[0])
	// などとした場合、サイズが合わなければ結局割り当てなおしが必要なわけで…
	// 普通に最初からコピーの割り当てでもいいか
	d := v.Interface().(*Data)
	d.Key = r.table.key.copyValue(r.Key())
	if d.Columns != nil {
		d.Columns = d.Columns[:0]
	}
	for _, col := range r.table.columns {
		cv := col.copyValue(r.Column(col.Name()))
		d.Columns = append(d.Columns, cv)
	}
	return nil
}

func isTaggedStruct(t reflect.Type) bool {
	if t.Kind() != reflect.Struct {
		return false
	}
	for _, f := range reflect.VisibleFields(t) {
		_, ok := f.Tag.Lookup(structTagKey)
		if ok {
			return true
		}
	}
	return false
}

func moveDataToTaggedStruct(r *Record, st any) error {
	v := reflect.ValueOf(st)
	for v.Kind() == reflect.Pointer {
		if v.IsNil() {
			if isTaggedStruct(v.Type().Elem()) && v.CanSet() {
				v.Set(reflect.New(v.Type().Elem()))
			} else {
				return errNotStruct
			}
		}
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return errNotStruct
	}
	for _, f := range reflect.VisibleFields(v.Type()) {
		tv, ok := f.Tag.Lookup(structTagKey)
		if !ok {
			continue
		}
		ft := f.Type
		for ft.Kind() == reflect.Pointer {
			ft = ft.Elem()
		}
		index := strings.LastIndex(tv, ",")
		mKey := tv
		var (
			err  error
			ct   ColumnType = invalidColumnType
			size uint64     = 0
		)
		if index < 0 {
			_, _, err = inferColumnType(ft)
			if err != nil {
				return TagError{fmt.Errorf("%w (field: %s)", err, f.Name)}
			}
		} else {
			mKey = tv[:index]
			_, ct, size, err = parseTagColumnType(tv[index+1:])
			if err != nil {
				return TagError{fmt.Errorf("%w (field: %s)", err, f.Name)}
			}
			if !canConvertToColumnType(ft, ct, size) {
				return TagError{fmt.Errorf("cannot convert type %s to %s (field: %s)", ft, ct.GoTypeHint(), f.Name)}
			}
		}
		if len(mKey) == 0 {
			mKey = f.Name
		}
		rv := r.Column(mKey)
		if rv == nil {
			return TagError{fmt.Errorf(`not found column "%s" (field: %s)`, mKey, f.Name)}
		}
		var col Column
		if r.table.key.Name() == mKey {
			col = r.table.key
		} else {
			for _, c := range r.table.columns {
				if c.Name() != mKey {
					continue
				}
				col = c
				break
			}
		}
		if ct != invalidColumnType {
			if col.Type() != ct {
				return TagError{fmt.Errorf("umatch column type (field: %s)", f.Name)}
			}
			if size > 0 && size != col.MaximumDataByteSize() {
				return TagError{fmt.Errorf("umatch column type (field: %s)", f.Name)}
			}
		}
		fv := v.FieldByIndex(f.Index)
		err = tryMoveDataValue(fv, rv, col)
		if err != nil {
			return TagError{fmt.Errorf("%w (field: %s)", err, f.Name)}
		}
	}
	return nil
}

func fillDataToTaggedStruct(r *Record, st any) error {
	v := reflect.ValueOf(st)
	for v.Kind() == reflect.Pointer {
		if v.IsNil() {
			if isTaggedStruct(v.Type().Elem()) && v.CanSet() {
				v.Set(reflect.New(v.Type().Elem()))
			} else {
				return errNotStruct
			}
		}
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return errNotStruct
	}
	for _, f := range reflect.VisibleFields(v.Type()) {
		tv, ok := f.Tag.Lookup(structTagKey)
		if !ok {
			continue
		}
		ft := f.Type
		for ft.Kind() == reflect.Pointer {
			ft = ft.Elem()
		}
		index := strings.LastIndex(tv, ",")
		mKey := tv
		var (
			err  error
			ct   ColumnType = invalidColumnType
			size uint64     = 0
		)
		if index < 0 {
			_, _, err = inferColumnType(ft)
			if err != nil {
				return TagError{fmt.Errorf("%w (field: %s)", err, f.Name)}
			}
		} else {
			mKey = tv[:index]
			_, ct, size, err = parseTagColumnType(tv[index+1:])
			if err != nil {
				return TagError{fmt.Errorf("%w (field: %s)", err, f.Name)}
			}
			if !canConvertToColumnType(ft, ct, size) {
				return TagError{fmt.Errorf("cannot convert type %s to %s (field: %s)", ft, ct.GoTypeHint(), f.Name)}
			}
		}
		if len(mKey) == 0 {
			mKey = f.Name
		}
		rv := r.Column(mKey)
		if rv == nil {
			return TagError{fmt.Errorf(`not found column "%s" (field: %s)`, mKey, f.Name)}
		}
		var col Column
		if r.table.key.Name() == mKey {
			col = r.table.key
		} else {
			for _, c := range r.table.columns {
				if c.Name() != mKey {
					continue
				}
				col = c
				break
			}
		}
		if ct != invalidColumnType {
			if col.Type() != ct {
				return TagError{fmt.Errorf("umatch column type (field: %s)", f.Name)}
			}
			if size > 0 && size != col.MaximumDataByteSize() {
				return TagError{fmt.Errorf("umatch column type (field: %s)", f.Name)}
			}
		}
		fv := v.FieldByIndex(f.Index)
		err = tryFillDataValue(fv, rv, col)
		if err != nil {
			return TagError{fmt.Errorf("%w (field: %s)", err, f.Name)}
		}
	}
	return nil
}

func tryMoveDataValue(fv reflect.Value, rv any, col Column) error {
	for fv.Kind() == reflect.Pointer {
		if fv.IsNil() {
			return ErrCannotAssignValueToField
		}
		fv = fv.Elem()
	}
	if !fv.CanSet() {
		return ErrCannotAssignValueToField
	}
	switch col.Type() {
	default:
		bug.Panic("UNREACHABLE")
	case Counter, Int8, Uint8, Int16, Uint16, Int32, Uint32, Int64, Uint64, Float32, Float64:
		value := reflect.ValueOf(rv)
		if fv.Kind() == value.Kind() {
			fv.Set(value)
		} else if value.CanConvert(fv.Type()) {
			fv.Set(value.Convert(fv.Type()))
		} else {
			return ErrCannotAssignValueToField
		}
	case ShortString, FixedSizeShortString, LongString, FixedSizeLongString, Text:
		if fv.Kind() == reflect.String {
			fv.Set(reflect.ValueOf(rv))
		} else {
			return ErrCannotAssignValueToField
		}
	case ShortBytes, FixedSizeShortBytes, LongBytes, FixedSizeLongBytes, Blob:
		if fv.Kind() == reflect.Slice && fv.Type().Elem().Kind() == reflect.Uint8 {
			fv.Set(reflect.ValueOf(rv))
		} else if fv.Kind() == reflect.Array && fv.Type().Elem().Kind() == reflect.Uint8 {
			if fv.Len() == len(rv.([]byte)) {
				// 固定長配列へのコピーを許容するのはアリなの？
				copy(fv.Slice(0, fv.Len()).Bytes(), rv.([]byte))
			} else {
				return ErrCannotAssignValueToField
			}
		} else {
			return ErrCannotAssignValueToField
		}
	}
	return nil
}

func tryFillDataValue(fv reflect.Value, rv any, col Column) error {
	for fv.Kind() == reflect.Pointer {
		if fv.IsNil() {
			return ErrCannotAssignValueToField
		}
		fv = fv.Elem()
	}
	if !fv.CanSet() {
		return ErrCannotAssignValueToField
	}
	switch col.Type() {
	default:
		bug.Panic("UNREACHABLE")
	case Counter, Int8, Uint8, Int16, Uint16, Int32, Uint32, Int64, Uint64, Float32, Float64:
		value := reflect.ValueOf(rv)
		if fv.Kind() == value.Kind() {
			fv.Set(value)
		} else if value.CanConvert(fv.Type()) {
			fv.Set(value.Convert(fv.Type()))
		} else {
			return ErrCannotAssignValueToField
		}
	case ShortString, FixedSizeShortString, LongString, FixedSizeLongString, Text:
		if fv.Kind() == reflect.String {
			fv.Set(reflect.ValueOf(rv))
		} else {
			return ErrCannotAssignValueToField
		}
	case ShortBytes, FixedSizeShortBytes, LongBytes, FixedSizeLongBytes, Blob:
		if fv.Kind() == reflect.Slice && fv.Type().Elem().Kind() == reflect.Uint8 {
			// 本当にこれコピーする必要あるの？これ無駄処理ぽそう
			buf := append(fv.Bytes()[:0], rv.([]byte)...)
			fv.SetBytes(buf)
		} else if fv.Kind() == reflect.Array && fv.Type().Elem().Kind() == reflect.Uint8 {
			if fv.Len() == len(rv.([]byte)) {
				// 固定長配列へのコピーを許容するのはアリなの？
				copy(fv.Slice(0, fv.Len()).Bytes(), rv.([]byte))
			} else {
				return ErrCannotAssignValueToField
			}
		} else {
			return ErrCannotAssignValueToField
		}
	}
	return nil
}

func createTableByTaggedStruct(tc *TableCreator, st any) error {
	t := reflect.TypeOf(st)
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return errNotStruct
	}
	hasKey := false
	m := make(map[string]bool)
	for _, f := range reflect.VisibleFields(t) {
		tv, ok := f.Tag.Lookup(structTagKey)
		if !ok {
			continue
		}
		ft := f.Type
		for ft.Kind() == reflect.Pointer {
			ft = ft.Elem()
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
			ct, size, err = inferColumnType(ft)
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
			if !canConvertToColumnType(ft, ct, size) {
				return TagError{fmt.Errorf("cannot convert type %s to %s (field: %s)", ft, ct.GoTypeHint(), f.Name)}
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
		return ErrNotFoundKey
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
			err = tc.Int64Key(mKey)
		} else {
			err = tc.Int64Column(mKey)
		}
	case Uint64:
		if isKey {
			err = tc.Uint64Key(mKey)
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

func canConvertToColumnType(t reflect.Type, ct ColumnType, size uint64) (_ bool) {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	switch ct {
	default:
		bug.Panic("UNREACHABLE")
	case Counter, Uint32:
		return t.Kind() == reflect.Uint32 ||
			t.ConvertibleTo(reflect.TypeOf(uint32(0)))
	case Int8:
		return t.Kind() == reflect.Int8 ||
			t.ConvertibleTo(reflect.TypeOf(int8(0)))
	case Uint8:
		return t.Kind() == reflect.Uint8 ||
			t.ConvertibleTo(reflect.TypeOf(uint8(0)))
	case Int16:
		return t.Kind() == reflect.Int16 ||
			t.ConvertibleTo(reflect.TypeOf(int16(0)))
	case Uint16:
		return t.Kind() == reflect.Uint16 ||
			t.ConvertibleTo(reflect.TypeOf(uint16(0)))
	case Int32:
		return t.Kind() == reflect.Int32 ||
			t.ConvertibleTo(reflect.TypeOf(int32(0)))
	case Int64:
		return t.Kind() == reflect.Int64 ||
			t.ConvertibleTo(reflect.TypeOf(int64(0)))
	case Uint64:
		return t.Kind() == reflect.Uint64 ||
			t.ConvertibleTo(reflect.TypeOf(uint64(0)))
	case Float32:
		return t.Kind() == reflect.Float32 ||
			t.ConvertibleTo(reflect.TypeOf(float32(0)))
	case Float64:
		return t.Kind() == reflect.Float64 ||
			t.ConvertibleTo(reflect.TypeOf(float64(0)))
	case ShortString, FixedSizeShortString, LongString, FixedSizeLongString, Text:
		return t.Kind() == reflect.String
	case ShortBytes:
		if t.Kind() == reflect.Slice && t.Elem().Kind() == reflect.Uint8 {
			return true
		} else if t.Kind() == reflect.Array && t.Elem().Kind() == reflect.Uint8 {
			return t.Len() <= shortBytesMaximumDataByteSize
		}
	case FixedSizeShortBytes:
		if t.Kind() == reflect.Slice && t.Elem().Kind() == reflect.Uint8 {
			return true
		} else if t.Kind() == reflect.Array && t.Elem().Kind() == reflect.Uint8 {
			return uint64(t.Len()) == size
		}
	case LongBytes:
		if t.Kind() == reflect.Slice && t.Elem().Kind() == reflect.Uint8 {
			return true
		} else if t.Kind() == reflect.Array && t.Elem().Kind() == reflect.Uint8 {
			return t.Len() <= longBytesMaximumDataByteSize
		}
	case FixedSizeLongBytes:
		if t.Kind() == reflect.Slice && t.Elem().Kind() == reflect.Uint8 {
			return true
		} else if t.Kind() == reflect.Array && t.Elem().Kind() == reflect.Uint8 {
			return uint64(t.Len()) == size
		}
	case Blob:
		if t.Kind() == reflect.Slice && t.Elem().Kind() == reflect.Uint8 {
			return true
		} else if t.Kind() == reflect.Array && t.Elem().Kind() == reflect.Uint8 {
			return t.Len() <= blobMaximumDataByteSize
		}
	}
	return
}

func parseData(table *Table, data any) (tableTreeValue, error) {
	if data == nil {
		return nil, ErrNotFoundData
	}
	if m, ok := data.(tableTreeValue); ok {
		return m, nil
	}
	if m := parseDataStruct(table, data); m != nil {
		return m, nil
	}
	if m, err := parseTaggedStruct(data); err != errNotStruct {
		return m, err
	}
	v := reflect.ValueOf(data)
	for v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return nil, ErrNotFoundData
		}
		v = v.Elem()
	}
	if v.Kind() != reflect.Map || v.IsNil() {
		return nil, ErrNotFoundData
	}
	keyType := v.Type().Key()
	stringerType := reflect.TypeOf((*fmt.Stringer)(nil)).Elem()
	if keyType.Kind() != reflect.String && !keyType.Implements(stringerType) {
		return nil, ErrNotFoundData
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

func parseDataStruct(table *Table, data any) tableTreeValue {
	v := reflect.ValueOf(data)
	for v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return nil
		}
		v = v.Elem()
	}
	d, ok := v.Interface().(Data)
	if !ok {
		return nil
	}
	m := make(tableTreeValue)
	m[table.key.Name()] = d.Key
	for i, col := range table.columns {
		if i < len(d.Columns) {
			m[col.Name()] = d.Columns[i]
		}
	}
	return m
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

func tryConvertToColumnValue(v reflect.Value, ct ColumnType, size uint64) (r reflect.Value, ok bool) {
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

func parseTaggedStruct(st any) (tableTreeValue, error) {
	v := reflect.ValueOf(st)
	for v.Kind() == reflect.Pointer {
		// forじゃなくifがいいのだろうか・・・・？
		if v.IsNil() {
			return nil, ErrNotFoundData
		}
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return nil, errNotStruct
	}
	hasKey := false
	m := make(tableTreeValue)
	for _, f := range reflect.VisibleFields(v.Type()) {
		tv, ok := f.Tag.Lookup(structTagKey)
		if !ok {
			continue
		}
		value := v.FieldByIndex(f.Index)
		for value.Kind() == reflect.Pointer {
			if value.IsNil() {
				return nil, ErrNotFoundData
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
			value, ok = tryConvertToColumnValue(value, ct, size)
			if !ok {
				ft := f.Type
				for ft.Kind() == reflect.Pointer {
					ft = ft.Elem()
				}
				return nil, TagError{fmt.Errorf("cannot convert type %s to %s (field: %s)", ft, ct.GoTypeHint(), f.Name)}
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

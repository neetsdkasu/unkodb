// unkodb
// author: Leonardone @ NEETSDKASU

package unkodb

import (
	"reflect"
	"strings"
)

func parseStruct(st any) (map[string]any, error) {
	v := reflect.ValueOf(st)
	for v.Kind() == reflect.Pointer {
		// forじゃなくifがいいのだろうか・・・・？
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return nil, notStruct
	}
	m := make(map[string]any)
	for _, f := range reflect.VisibleFields(v.Type()) {
		tv, ok := f.Tag.Lookup("unkodb")
		if !ok {
			continue
		}
		index := strings.LastIndex(tv, ",")
		mKey := string([]byte(tv[:index]))
		if len(mKey) == 0 {
			mKey = f.Name
		}
		// TODO tvの,以降の指定での 型チェック
		m[mKey] = v.FieldByIndex(f.Index).Interface()
	}
	return m, nil
}

// unkodb
// author: Leonardone @ NEETSDKASU

package unkodb

// データのコピーを保持する。
type Record struct {
	table *Table
	data  tableTreeValue
}

// データ元のテーブルを参照する。
func (r *Record) Table() *Table {
	return r.table
}

// データのキーを参照する。
//
//	r, _ := table.Find(unkodb.CounterType(123))
//	fmt.Println("key=", r.Key(), "value=", r.Column("value"))
func (r *Record) Key() (value any) {
	value = r.data[r.table.key.Name()]
	return
}

// 指定カラム名のカラムの値を参照する。
// テーブルに存在しないカラム名の場合はnilが返る。
// キー名も指定できる。
//
//	r, _ := table.Find(unkodb.CounterType(123))
//	fmt.Println("id=", r.Column("id"), "name=", r.Column("name"))
func (r *Record) Column(name string) any {
	if value, ok := r.data[name]; ok {
		return value
	} else {
		return nil
	}
}

// カラムの値を参照するリストを返す。
// 値の順序はテーブルのカラムの順序と同じ。
func (r *Record) Columns() []any {
	list := make([]any, len(r.table.columns))
	for i, col := range r.table.columns {
		list[i] = r.data[col.Name()]
	}
	return list
}

// データの値をdstで渡した構造体に移動する。
// dstにはunkodb.Dataかunkodbタグ付きの構造体のインスタンスを指定する。
// データ移動後はこの*Recordは使用不可になる。
// 引数のdstに対応できない型などが渡された場合はエラーが返る。
//
//	r, _ := table.Find(unkodb.CounterType(123))
//	var dst unkodb.Data
//	r.MoveTo(&dst)
func (r *Record) MoveTo(dst any) (err error) {
	if !debugMode {
		defer catchError(&err)
	}
	err = moveData(r, dst)
	r.table = nil
	r.data = nil
	return
}

// データの値をdstで渡した構造体にコピーする。
// dstにはunkodb.Dataかunkodbタグ付きの構造体のインスタンスを指定する。
// 引数のdstに対応できない型などが渡された場合はエラーが返る。
//
//	r, _ := table.Find(unkodb.CounterType(123))
//	var dst1, dst2 unkodb.Data
//	r.CopyTo(&dst1)
//	r.CopyTo(&dst2)
func (r *Record) CopyTo(dst any) (err error) {
	if !debugMode {
		defer catchError(&err)
	}
	err = fillData(r, dst)
	return
}

// 内部で保持してるデータを取り出す。
// このメソッドの実行後はこの*Recordは使用不可になる。
//
//	r, _ := table.Find(unkodb.CounterType(123))
//	fmt.Println("id=", r.Column("id"), "name=", r.Column("name"))
//	data := r.Take()
//	fmt.Println("id=", data["id"], "name=", data["name"])
func (r *Record) Take() (data map[string]any) {
	data = r.data
	r.table = nil
	r.data = nil
	return
}

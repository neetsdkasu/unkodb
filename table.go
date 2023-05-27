// unkodb
// author: Leonardone @ NEETSDKASU

package unkodb

import (
	"github.com/neetsdkasu/avltree"
)

// データのイテレーションに使用するコールバック関数。
// イテレーションを中断したい場合には戻り値breakIterationにtrueを指定する。
type IterateCallbackFunc = func(r *Record) (breakIteration bool)

// キーのイテレーションに使用するコールバック関数。
// イテレーションを中断したい場合には戻り値breakIterationにtrueを指定する。
type IterateKeyCallbackFunc = func(key any) (breakIteration bool)

type dataSeparationState uint8

func (dss dataSeparationState) IsValid() bool {
	return dss == dataSeparationEnabled || dss == dataSeparationDisabled
}

func (dss dataSeparationState) Enabled() bool {
	return dss == dataSeparationEnabled
}

type Table struct {
	db             *UnkoDB
	name           string
	key            keyColumn
	columns        []Column
	nodeCount      int
	counter        uint
	columnsSpecBuf []byte
	rootAddress    int
	rootAccessor   rootAddressAccessor
	dataSeparation dataSeparationState
	iterating      int
}

// テーブル名を返す。
func (table *Table) Name() string {
	return table.name
}

// キーのカラム情報を返す。
func (table *Table) Key() Column {
	return table.key
}

// 指定したカラム名のカラム情報を返す。
// 指定したカラム名が存在しない場合はnilを返す。
// キー名も指定できる。
func (table *Table) Column(name string) Column {
	if table.key.Name() == name {
		return table.key
	}
	for _, col := range table.columns {
		if col.Name() == name {
			return col
		}
	}
	return nil
}

// このテーブルの全てのカラムのカラム情報をリストにして返す。
// このリストにはキーは含まれない。
func (table *Table) Columns() []Column {
	columns := make([]Column, len(table.columns))
	copy(columns, table.columns)
	return columns
}

func (table *Table) getRootAddress() (addr int, err error) {
	addr = table.rootAddress
	return
}
func (table *Table) setRootAddress(addr int) (err error) {
	table.rootAddress = addr
	return
}

func (table *Table) flush() (err error) {
	if table.columnsSpecBuf == nil {
		// TODO たぶん tableList （バグチェックのために確認する処理あったほうがいいかも）
		return
	}
	buf := table.columnsSpecBuf[:tableSpecHeaderByteSize]
	w := newByteEncoder(newByteSliceWriter(buf), fileByteOrder)
	err = w.Int32(int32(table.rootAddress))
	if err != nil {
		return
	}
	err = w.Int32(int32(table.nodeCount))
	if err != nil {
		return
	}
	err = w.Uint32(uint32(table.counter))
	if err != nil {
		return
	}
	err = w.Uint8(uint8(table.dataSeparation))
	if err != nil {
		return
	}
	data := make(map[string]any)
	data[tableListKeyName] = table.name
	data[tableListColumnName] = table.columnsSpecBuf
	_, err = table.db.tableList.Replace(data)
	return
}

// InsertやReplaceに渡すデータにおいて各カラムのデータの型に問題にないかを確認をする。
// 引数のdataにはmap[string]anyもしくはunkodb.Dataもしくはunkodbタグを付けた構造体のインスタンスを渡す。
// データに問題がある場合は戻り値のエラーにヒント（？）的な情報が返る。
func (table *Table) CheckData(data any) (err error) {
	if !debugMode {
		defer catchError(&err)
	}
	if data == nil {
		return NotFoundData
	}
	var mdata tableTreeValue
	mdata, err = parseData(table, data)
	if err != nil {
		return
	}
	if keyValue, ok := mdata[table.key.Name()]; !ok {
		return ErrNotFoundColumnName{table.key}
	} else if !table.key.IsValidValueType(keyValue) {
		return UnmatchColumnValueType{table.key}
	}
	for _, col := range table.columns {
		if colValue, ok := mdata[col.Name()]; !ok {
			return ErrNotFoundColumnName{col}
		} else if !col.IsValidValueType(colValue) {
			return UnmatchColumnValueType{col}
		}
	}
	return nil
}

func (table *Table) getKey(data map[string]any) avltree.Key {
	return table.key.toKey(data[table.key.Name()])
}

// 指定したキーに対応するデータを取得する。
// キーのカラム型に対応したGoの型で渡す必要がある。
// 指定したキーに対応するデータが存在しない場合には戻り値は全てnilとなる。
// キーの型が不正な場合は対応するエラーが返る。
// それ以外のエラー(IOエラーなど)がある場合は戻り値エラーにnil以外が返る。（たいていプログラムの実行に致命的なエラー）
func (table *Table) Find(key any) (r *Record, err error) {
	if !debugMode {
		defer catchError(&err)
	}
	if mdata, e := parseData(table, key); e == nil {
		// parseDataするのコスト高すぎる
		if k, ok := mdata[table.key.Name()]; ok {
			key = k
		}
	}
	if !table.key.IsValidValueType(key) {
		err = UnmatchColumnValueType{table.key}
		return
	}
	var tree *tableTree
	tree, err = newTableTree(table, true)
	if err != nil {
		return
	}
	node := avltree.Find(tree, table.key.toKey(key))
	if node == nil {
		return
	}
	r = &Record{
		table: table,
		data:  node.Value().(tableTreeValue),
	}
	return
}

func (table *Table) deleteAll() (err error) {
	if table.isIterating() {
		err = InvalidOperation
		return
	}
	var tree *tableTree
	tree, err = newTableTree(table, false)
	if err != nil {
		return
	}
	avltree.Clear(tree)
	err = tree.flush()
	if err != nil {
		return
	}
	table.counter = 0
	table.nodeCount = 0
	err = table.flush()
	return
}

// 指定したキーに対応するデータとキーを削除する。
// キーのカラム型に対応したGoの型で渡す必要がある。
// 指定したキーに対応するデータが存在しない場合には戻り値のエラーはNotFoundKeyとなる。
// それ以外のエラー(IOエラーなど)がある場合は戻り値エラーにnil以外が返る。（たいていプログラムの実行に致命的なエラー）
func (table *Table) Delete(key any) (err error) {
	if table.isIterating() {
		err = InvalidOperation
		return
	}
	if !debugMode {
		defer catchError(&err)
	}
	// parseDataするのコスト高すぎる
	if mdata, e := parseData(table, key); e == nil {
		if k, ok := mdata[table.key.Name()]; ok {
			key = k
		}
	}
	if !table.key.IsValidValueType(key) {
		err = UnmatchColumnValueType{table.key}
		return
	}
	var tree *tableTree
	tree, err = newTableTree(table, false)
	if err != nil {
		return
	}
	_, node := avltree.Delete(tree, table.key.toKey(key))
	if node == nil {
		err = NotFoundKey
		return
	}
	err = tree.flush()
	if err != nil {
		return
	}
	table.nodeCount--
	err = table.flush()
	return
}

// テーブルに存在するキーの数を返す。
func (table *Table) Count() int {
	return table.nodeCount
}

// キーのカラム型をCounterにしている場合に次にInsertするときに付与されるキーの値を取得できる。
// キーのカラム型がCounterではない場合はKeyIsNotCounterのエラーが返る。
func (table *Table) NextCounterID() (CounterType, error) {
	if table.key.Type() != Counter {
		return 0, KeyIsNotCounter
	}
	return CounterType(table.counter + 1), nil
}

// テーブルにデータを挿入する。
// 引数のdataにはmap[string]anyもしくはunkodb.Dataもしくはunkodbタグを付けた構造体のインスタンスを渡す。
// dataにはキーとカラムの全ての値をセットしておく必要がある。
// キーのカラム型がCounterの場合はセットされたキーの値は無視される。
// 戻り値の*Recordには挿入されたデータのコピーが入る。
// キーのカラム型がCounterの場合は戻り値の*Recordにキーがセットされるのでキーの確認ができる。
// キーが既にテーブルに存在する場合はKeyAlreadyExistsのエラーが返る。
// 引数のdataに不正がある場合は対応したエラーが返る。
// それ以外のエラー(IOエラーなど)がある場合は戻り値エラーにnil以外が返る。（たいていプログラムの実行に致命的なエラー）
//
//	data := map[string]any{
//		"id":     unkodb.CounterType(0),
//		"title":  "プログラミング入門の本",
//		"author": "プログラマーのティーチャー",
//		"genre":  "技術書",
//	}
//	r, _ := table.Insert(data)
//	fmt.Println("idは", r.Key(), "になりました")
func (table *Table) Insert(data any) (r *Record, err error) {
	if table.isIterating() {
		err = InvalidOperation
		return
	}
	if !debugMode {
		defer catchError(&err)
	}
	var mdata tableTreeValue
	mdata, err = parseData(table, data)
	if err != nil {
		return
	}
	if table.key.Type() == Counter {
		if oldKey, ok := mdata[table.key.Name()]; ok {
			defer func() {
				mdata[table.key.Name()] = oldKey
			}()
		} else {
			defer func() {
				delete(mdata, table.key.Name())
			}()
		}
		mdata[table.key.Name()] = uint32(table.counter) + 1
	}
	err = table.CheckData(mdata)
	if err != nil {
		return
	}
	var tree *tableTree
	tree, err = newTableTree(table, false)
	if err != nil {
		return
	}
	key := table.getKey(mdata)
	_, ok := avltree.Insert(tree, false, key, tableTreeValue(mdata))
	if !ok {
		err = KeyAlreadyExists // duplicate key error
		return
	}
	err = tree.flush()
	if err != nil {
		return
	}
	table.nodeCount += 1
	if table.key.Type() == Counter {
		table.counter += 1
	}
	err = table.flush()
	node := avltree.Find(tree, key)
	if node == nil {
		bug.Panic("why? not found node")
	}
	r = &Record{
		table: table,
		data:  node.Value().(tableTreeValue),
	}
	return
}

// キーに対応するデータを置き換える。
// 引数のdataにはmap[string]anyもしくはunkodb.Dataもしくはunkodbタグを付けた構造体のインスタンスを渡す。
// dataにはキーとカラムの全てをセットしておく必要がある。
// dataのキーに対応するデータを置き換えることになる。
// 戻り値の*Recordには置換後のデータのコピーが入る。
// 対応するキーが存在しない場合はNotFoundKeyのエラーが返る。
// 引数のdataに不正がある場合は対応したエラーが返る。
// それ以外のエラー(IOエラーなど)がある場合は戻り値エラーにnil以外が返る。（たいていプログラムの実行に致命的なエラー）
//
//	r, _ := table.Find(unkodb.CounterType(123))
//	m := r.Take()
//	m["value"] = m["value"].(int32) + 99
//	table.Replace(m)
func (table *Table) Replace(data any) (r *Record, err error) {
	if table.isIterating() {
		err = InvalidOperation
		return
	}
	if !debugMode {
		defer catchError(&err)
	}
	var mdata tableTreeValue
	mdata, err = parseData(table, data)
	err = table.CheckData(mdata)
	if err != nil {
		return
	}
	var tree *tableTree
	tree, err = newTableTree(table, false)
	if err != nil {
		return
	}
	key := table.getKey(mdata)
	_, ok := avltree.Replace(tree, key, mdata)
	if !ok {
		err = NotFoundKey
		return
	}
	err = tree.flush()
	node := avltree.Find(tree, key)
	if node == nil {
		bug.Panic("why? not found node")
	}
	r = &Record{
		table: table,
		data:  node.Value().(tableTreeValue),
	}
	return
}

func (table *Table) isIterating() bool {
	return table.iterating > 0
}

func (table *Table) beginIteration() {
	table.iterating++
}

func (table *Table) endIteration() {
	table.iterating--
}

// テーブルに存在するデータのコピーをキーの昇順でコールバック関数に渡していく。
// イテレーション中はInsert/Replace/Delete/DeleteTableなどのテーブル変更操作を行うとデータが壊れる。
// エラー(IOエラーなど)がある場合は戻り値エラーにnil以外が返る。（たいていプログラムの実行に致命的なエラー）
// コールバック関数内でのpanicはエラーとして返ることがある。（その場合、スタックトレース取得などはコールバック関数内で頑張って）。
//
//	table.IterateAll(func(r *unkodb.Record) (breakIteration bool) {
//		if r.Column("value").(int32) == 123 {
//			fmt.Println("valueが123となる最初のキーは", r.Key(), "です")
//			// IterateAllを中断する
//			breakIteration = true
//		}
//		return
//	})
func (table *Table) IterateAll(callback IterateCallbackFunc) (err error) {
	if !debugMode {
		defer catchError(&err)
	}
	table.beginIteration()
	defer table.endIteration()
	tree, err := newTableTree(table, true)
	if err != nil {
		return err
	}
	avltree.Iterate(tree, false, func(node avltree.Node) (breakIteration bool) {
		rec := &Record{
			table: table,
			data:  node.Value().(tableTreeValue),
		}
		return callback(rec)
	})
	return
}

// テーブルに存在するデータのコピーをキーの降順でコールバック関数に渡していく。
// イテレーション中はInsert/Replace/Delete/DeleteTableなどのテーブル変更操作を行うとデータが壊れる。
// エラー(IOエラーなど)がある場合は戻り値エラーにnil以外が返る。（たいていプログラムの実行に致命的なエラー）
// コールバック関数内でのpanicはエラーとして返ることがある。（その場合、スタックトレース取得などはコールバック関数内で頑張って）。
//
//	table.IterateBackAll(func(r *unkodb.Record) (breakIteration bool) {
//		if r.Column("value").(int32) == 123 {
//			fmt.Println("valueが123となる最後のキーは", r.Key(), "です")
//			// IterateBackAllを中断する
//			breakIteration = true
//		}
//		return
//	})
func (table *Table) IterateBackAll(callback IterateCallbackFunc) (err error) {
	if !debugMode {
		defer catchError(&err)
	}
	table.beginIteration()
	defer table.endIteration()
	tree, err := newTableTree(table, true)
	if err != nil {
		return err
	}
	avltree.Iterate(tree, true, func(node avltree.Node) (breakIteration bool) {
		rec := &Record{
			table: table,
			data:  node.Value().(tableTreeValue),
		}
		return callback(rec)
	})
	return
}

// テーブルの指定範囲内に存在するデータのコピーをキーの昇順でコールバック関数に渡していく。
// lowerKey以上upperKey以下のキーの範囲のデータを辿る。
// キーの指定にはキーのカラム型に合ったGoの型で指定する必要がある。
// イテレーション中はInsert/Replace/Delete/DeleteTableなどのテーブル変更操作を行うとデータが壊れる。
// エラー(IOエラーなど)がある場合は戻り値エラーにnil以外が返る。（たいていプログラムの実行に致命的なエラー）
// コールバック関数内でのpanicはエラーとして返ることがある。（その場合、スタックトレース取得などはコールバック関数内で頑張って）。
//
//	lowerKey := unkodb.Counter(1000)
//	upperKey := unkodb.Counter(1999)
//	table.IterateRange(lowerKey, upperKey, func(r *unkodb.Record) (breakIteration bool) {
//		if r.Column("value").(int32) == 123 {
//			fmt.Println("valueが123となる最初のキーは", r.Key(), "です")
//			// IterateRangeを中断する
//			breakIteration = true
//		}
//		return
//	})
func (table *Table) IterateRange(lowerKey, upperKey any, callback IterateCallbackFunc) (err error) {
	if !debugMode {
		defer catchError(&err)
	}
	table.beginIteration()
	defer table.endIteration()
	var lKey, rKey avltree.Key
	if lowerKey != nil {
		if table.key.IsValidValueType(lowerKey) {
			lKey = table.key.toKey(lowerKey)
		} else {
			err = UnmatchColumnValueType{table.key}
			return
		}
	}
	if upperKey != nil {
		if table.key.IsValidValueType(upperKey) {
			rKey = table.key.toKey(upperKey)
		} else {
			err = UnmatchColumnValueType{table.key}
			return
		}
	}
	tree, err := newTableTree(table, true)
	if err != nil {
		return err
	}
	avltree.RangeIterate(tree, false, lKey, rKey, func(node avltree.Node) (breakIteration bool) {
		rec := &Record{
			table: table,
			data:  node.Value().(tableTreeValue),
		}
		return callback(rec)
	})
	return
}

// テーブルの指定範囲内に存在するデータのコピーをキーの降順でコールバック関数に渡していく。
// lowerKey以上upperKey以下のキーの範囲のデータを辿る。
// キーの指定にはキーのカラム型に合ったGoの型で指定する必要がある。
// イテレーション中はInsert/Replace/Delete/DeleteTableなどのテーブル変更操作を行うとデータが壊れる。
// エラー(IOエラーなど)がある場合は戻り値エラーにnil以外が返る。（たいていプログラムの実行に致命的なエラー）
// コールバック関数内でのpanicはエラーとして返ることがある。（その場合、スタックトレース取得などはコールバック関数内で頑張って）。
//
//	lowerKey := unkodb.Counter(1000)
//	upperKey := unkodb.Counter(1999)
//	table.IterateBackRange(lowerKey, upperKey, func(r *unkodb.Record) (breakIteration bool) {
//		if r.Column("value").(int32) == 123 {
//			fmt.Println("valueが123となる最後のキーは", r.Key(), "です")
//			// IterateBackRangeを中断する
//			breakIteration = true
//		}
//		return
//	})
func (table *Table) IterateBackRange(lowerKey, upperKey any, callback IterateCallbackFunc) (err error) {
	if !debugMode {
		defer catchError(&err)
	}
	table.beginIteration()
	defer table.endIteration()
	var lKey, rKey avltree.Key
	if lowerKey != nil {
		if table.key.IsValidValueType(lowerKey) {
			lKey = table.key.toKey(lowerKey)
		} else {
			err = UnmatchColumnValueType{table.key}
			return
		}
	}
	if upperKey != nil {
		if table.key.IsValidValueType(upperKey) {
			rKey = table.key.toKey(upperKey)
		} else {
			err = UnmatchColumnValueType{table.key}
			return
		}
	}
	tree, err := newTableTree(table, true)
	if err != nil {
		return err
	}
	avltree.RangeIterate(tree, true, lKey, rKey, func(node avltree.Node) (breakIteration bool) {
		rec := &Record{
			table: table,
			data:  node.Value().(tableTreeValue),
		}
		return callback(rec)
	})
	return
}

// テーブルに存在するキーのコピーを昇順でコールバック関数に渡していく。
// イテレーション中はInsert/Replace/Delete/DeleteTableなどのテーブル変更操作を行うとデータが壊れる。
// エラー(IOエラーなど)がある場合は戻り値エラーにnil以外が返る。（たいていプログラムの実行に致命的なエラー）
// コールバック関数内でのpanicはエラーとして返ることがある。（その場合、スタックトレース取得などはコールバック関数内で頑張って）。
//
//	table.IterateAllKeys(func(key any) (breakIteration bool) {
//		if key.(int32) > 123 {
//			fmt.Println("123を超える最初のキーは", r.Key(), "です")
//			// IterateAllKeysを中断する
//			breakIteration = true
//		}
//		return
//	})
func (table *Table) IterateAllKeys(callback IterateKeyCallbackFunc) (err error) {
	if !debugMode {
		defer catchError(&err)
	}
	table.beginIteration()
	defer table.endIteration()
	tree, err := newTableTree(table, true)
	if err != nil {
		return err
	}
	avltree.Iterate(tree, false, func(node avltree.Node) (breakIteration bool) {
		key := table.key.copyValue(table.key.unwrapKey(node.Key()))
		return callback(key)
	})
	return
}

// テーブルに存在するキーのコピーを降順でコールバック関数に渡していく。
// イテレーション中はInsert/Replace/Delete/DeleteTableなどのテーブル変更操作を行うとデータが壊れる。
// エラー(IOエラーなど)がある場合は戻り値エラーにnil以外が返る。（たいていプログラムの実行に致命的なエラー）
// コールバック関数内でのpanicはエラーとして返ることがある。（その場合、スタックトレース取得などはコールバック関数内で頑張って）。
//
//	table.IterateBackAllKeys(func(r *unkodb.Record) (breakIteration bool) {
//		if key.(int32) < 123 {
//			fmt.Println("123未満の最後のキーは", r.Key(), "です")
//			// IterateBackAllKeysを中断する
//			breakIteration = true
//		}
//		return
//	})
func (table *Table) IterateBackAllKeys(callback IterateKeyCallbackFunc) (err error) {
	if !debugMode {
		defer catchError(&err)
	}
	table.beginIteration()
	defer table.endIteration()
	tree, err := newTableTree(table, true)
	if err != nil {
		return err
	}
	avltree.Iterate(tree, true, func(node avltree.Node) (breakIteration bool) {
		key := table.key.copyValue(table.key.unwrapKey(node.Key()))
		return callback(key)
	})
	return
}

// テーブルの指定範囲内に存在するキーのコピーを昇順でコールバック関数に渡していく。
// lowerKey以上upperKey以下の範囲のキーを辿る。
// キーの指定にはキーのカラム型に合ったGoの型で指定する必要がある。
// イテレーション中はInsert/Replace/Delete/DeleteTableなどのテーブル変更操作を行うとデータが壊れる。
// エラー(IOエラーなど)がある場合は戻り値エラーにnil以外が返る。（たいていプログラムの実行に致命的なエラー）
// コールバック関数内でのpanicはエラーとして返ることがある。（その場合、スタックトレース取得などはコールバック関数内で頑張って）。
//
//	lowerKey := int32(100)
//	upperKey := int32(199)
//	table.IterateRangeKeys(lowerKey, upperKey, func(key any) (breakIteration bool) {
//		if key.(int32) % 7 == 0 {
//			fmt.Println("7の倍数である最初のキーは", r.Key(), "です")
//			// IterateRangeKeysを中断する
//			breakIteration = true
//		}
//		return
//	})
func (table *Table) IterateRangeKeys(lowerKey, upperKey any, callback IterateKeyCallbackFunc) (err error) {
	if !debugMode {
		defer catchError(&err)
	}
	table.beginIteration()
	defer table.endIteration()
	var lKey, rKey avltree.Key
	if lowerKey != nil {
		if table.key.IsValidValueType(lowerKey) {
			lKey = table.key.toKey(lowerKey)
		} else {
			err = UnmatchColumnValueType{table.key}
			return
		}
	}
	if upperKey != nil {
		if table.key.IsValidValueType(upperKey) {
			rKey = table.key.toKey(upperKey)
		} else {
			err = UnmatchColumnValueType{table.key}
			return
		}
	}
	tree, err := newTableTree(table, true)
	if err != nil {
		return err
	}
	avltree.RangeIterate(tree, false, lKey, rKey, func(node avltree.Node) (breakIteration bool) {
		key := table.key.copyValue(table.key.unwrapKey(node.Key()))
		return callback(key)
	})
	return
}

// テーブルの指定範囲内に存在するキーのコピーを降順でコールバック関数に渡していく。
// lowerKey以上upperKey以下の範囲のキーを辿る。
// キーの指定にはキーのカラム型に合ったGoの型で指定する必要がある。
// イテレーション中はInsert/Replace/Delete/DeleteTableなどのテーブル変更操作を行うとデータが壊れる。
// エラー(IOエラーなど)がある場合は戻り値エラーにnil以外が返る。（たいていプログラムの実行に致命的なエラー）
// コールバック関数内でのpanicはエラーとして返ることがある。（その場合、スタックトレース取得などはコールバック関数内で頑張って）。
//
//	lowerKey := int32(100)
//	upperKey := int32(199)
//	table.IterateBackRangeKeys(lowerKey, upperKey, func(key any) (breakIteration bool) {
//		if key.(int32) % 7 == 0 {
//			fmt.Println("7の倍数である最後のキーは", r.Key(), "です")
//			// IterateBackRangeKeysを中断する
//			breakIteration = true
//		}
//		return
//	})
func (table *Table) IterateBackRangeKeys(lowerKey, upperKey any, callback IterateKeyCallbackFunc) (err error) {
	if !debugMode {
		defer catchError(&err)
	}
	table.beginIteration()
	defer table.endIteration()
	var lKey, rKey avltree.Key
	if lowerKey != nil {
		if table.key.IsValidValueType(lowerKey) {
			lKey = table.key.toKey(lowerKey)
		} else {
			err = UnmatchColumnValueType{table.key}
			return
		}
	}
	if upperKey != nil {
		if table.key.IsValidValueType(upperKey) {
			rKey = table.key.toKey(upperKey)
		} else {
			err = UnmatchColumnValueType{table.key}
			return
		}
	}
	tree, err := newTableTree(table, true)
	if err != nil {
		return err
	}
	avltree.RangeIterate(tree, true, lKey, rKey, func(node avltree.Node) (breakIteration bool) {
		key := table.key.copyValue(table.key.unwrapKey(node.Key()))
		return callback(key)
	})
	return
}

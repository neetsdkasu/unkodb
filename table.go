// unkodb
// author: Leonardone @ NEETSDKASU

package unkodb

import (
	"github.com/neetsdkasu/avltree"
)

type IterateCallbackFunc = func(r *Record) (breakIteration bool)

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
		return NotFoundColumnName{table.key}
	} else if !table.key.IsValidValueType(keyValue) {
		return UnmatchColumnValueType{table.key}
	}
	for _, col := range table.columns {
		if colValue, ok := mdata[col.Name()]; !ok {
			return NotFoundColumnName{col}
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
	tree, err = newTableTree(table)
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
	var tree *tableTree
	tree, err = newTableTree(table)
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
// 指定したキーに対応するデータが存在しない場合には戻り値はNotFoundKeyとなる。
func (table *Table) Delete(key any) (err error) {
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
	tree, err = newTableTree(table)
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
// キーが既にテーブルに存在する場合はKeyAlreadyExistsのエラーが返る。
// キーのカラム型がCounterの場合はセットされたキーの値は無視される。
// 戻り値の*Recordには挿入されたデータのコピーが入る。
// キーのカラム型がCounterの場合は戻り値の*Recordにキーがセットされるのでキーの確認ができる。
//
// 		data := map[string]any{
// 			"id":     unkodb.CounterType(0),
// 			"title":  "プログラミング入門の本",
// 			"author": "プログラマーのティーチャー",
// 			"genre":  "技術書",
// 		}
// 		r, _ := table.Insert(data)
// 		fmt.Println("idは", r.Key(), "になりました")
//
func (table *Table) Insert(data any) (r *Record, err error) {
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
	tree, err = newTableTree(table)
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
// 対応するキーが存在しない場合はNotFoundKeyのエラーが返る。
func (table *Table) Replace(data any) (r *Record, err error) {
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
	tree, err = newTableTree(table)
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

// テーブルに存在するデータをキーの昇順でコールバック関数に渡していく。
//
// 		table.IterateAll(func(r *unkodb.Record) (breakIteration bool) {
// 			if r.Column("value").(int32) == 123 {
// 				fmt.Println("valueが123となる最初のキーは", r.Key(), "です")
// 				// IterateAllを中断する
// 				breakIteration = true
// 			}
// 			return
// 		})
//
func (table *Table) IterateAll(callback IterateCallbackFunc) (err error) {
	if !debugMode {
		defer catchError(&err)
	}
	tree, err := newTableTree(table)
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

// テーブルに存在するデータをキーの降順でコールバック関数に渡していく。
//
// 		table.IterateBackAll(func(r *unkodb.Record) (breakIteration bool) {
// 			if r.Column("value").(int32) == 123 {
// 				fmt.Println("valueが123となる最後のキーは", r.Key(), "です")
// 				// IterateBackAllを中断する
// 				breakIteration = true
// 			}
// 			return
// 		})
//
func (table *Table) IterateBackAll(callback IterateCallbackFunc) (err error) {
	if !debugMode {
		defer catchError(&err)
	}
	tree, err := newTableTree(table)
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

func (table *Table) IterateRange(lowerKey, upperKey any, callback IterateCallbackFunc) (err error) {
	if !debugMode {
		defer catchError(&err)
	}
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
	tree, err := newTableTree(table)
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

func (table *Table) IterateBackRange(lowerKey, upperKey any, callback IterateCallbackFunc) (err error) {
	if !debugMode {
		defer catchError(&err)
	}
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
	tree, err := newTableTree(table)
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

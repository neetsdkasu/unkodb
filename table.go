// unkodb
// author: Leonardone @ NEETSDKASU

package unkodb

import (
	"github.com/neetsdkasu/avltree"
)

type Record struct {
	table *Table
	data  tableTreeValue
}

func (r *Record) Table() *Table {
	return r.table
}

func (r *Record) Key() (value any) {
	value = r.data[r.table.key.Name()]
	return
}

func (r *Record) Column(name string) any {
	if value, ok := r.data[name]; ok {
		return value
	} else {
		return nil
	}
}

type Table struct {
	db             *UnkoDB
	name           string
	key            keyColumn
	columns        []Column
	columnsSpecBuf []byte
	rootAddress    int
	rootAccessor   rootAddressAccessor
}

func (table *Table) Name() string {
	return table.name
}

func (table *Table) Key() Column {
	return table.key
}

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
	buf := table.columnsSpecBuf[:addressByteSize]
	w := newByteEncoder(newByteSliceWriter(buf), fileByteOrder)
	err = w.Int32(int32(addr))
	if err != nil {
		return
	}
	data := make(map[string]any)
	data[tableListKeyName] = table.name
	data[tableListColumnName] = table.columnsSpecBuf
	err = table.db.tableList.Replace(data)
	return
}

func (table *Table) CheckData(data map[string]any) error {
	if data == nil {
		// TODO error
	}
	if keyValue, ok := data[table.key.Name()]; !ok {
		// TODO error
	} else if !table.key.IsValidValueType(keyValue) {
		// TODO error
	}
	for _, col := range table.columns {
		if colValue, ok := data[col.Name()]; !ok {
			// TODO error
		} else if !col.IsValidValueType(colValue) {
			// TODO error
		}
	}
	return nil
}

func (table *Table) getKey(data map[string]any) avltree.Key {
	return table.key.toKey(data[table.key.Name()])
}

func (table *Table) Insert(data map[string]any) (err error) {
	defer catchError(&err)
	err = table.CheckData(data)
	if err != nil {
		return
	}
	var tree *tableTree
	tree, err = newTableTree(table)
	if err != nil {
		return
	}
	key := table.getKey(data)
	_, ok := avltree.Insert(tree, false, key, tableTreeValue(data))
	if !ok {
		// TODO duplicate key error
	}
	err = tree.flush()
	return
}

func (table *Table) Replace(data map[string]any) (err error) {
	defer catchError(&err)
	err = table.CheckData(data)
	if err != nil {
		return
	}
	var tree *tableTree
	tree, err = newTableTree(table)
	if err != nil {
		return
	}
	key := table.getKey(data)
	_, ok := avltree.Replace(tree, key, tableTreeValue(data))
	if !ok {
		bug.Panic("table.Replace: why?")
	}
	err = tree.flush()
	return
}

func (table *Table) IterateAll(callback func(record *Record) (breakIteration bool)) (err error) {
	defer catchError(&err)
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

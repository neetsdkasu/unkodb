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
	db           *UnkoDB
	name         string
	key          keyColumn
	columns      []Column
	rootAccessor rootAddressAccessor
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

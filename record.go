// unkodb
// author: Leonardone @ NEETSDKASU

package unkodb

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

func (r *Record) Get(name string) (value any, ok bool) {
	value, ok = r.data[name]
	return
}

func (r *Record) Column(name string) any {
	if value, ok := r.data[name]; ok {
		return value
	} else {
		return nil
	}
}

func (r *Record) Columns() []any {
	list := make([]any, len(r.table.columns))
	for i, col := range r.table.columns {
		list[i] = r.data[col.Name()]
	}
	return list
}

func (r *Record) MoveTo(dst any) (err error) {
	if !debugMode {
		defer catchError(&err)
	}
	err = moveData(r, dst)
	r.table = nil
	r.data = nil
	return
}

func (r *Record) CopyTo(dst any) (err error) {
	if !debugMode {
		defer catchError(&err)
	}
	err = fillData(r, dst)
	return
}

func (r *Record) Take() (data map[string]any) {
	data = r.data
	r.table = nil
	r.data = nil
	return
}

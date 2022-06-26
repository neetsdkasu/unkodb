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

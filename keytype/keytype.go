package keytype

type KeyType int

const (
	CountId KeyType = iota
	Int32Key
	Int32Id
	Int64Key
	Int64Id
	Bytes
	String
)

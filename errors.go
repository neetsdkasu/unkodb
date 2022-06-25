// unkodb
// author: Leonardone @ NEETSDKASU

package unkodb

import "errors"

type NotFoundColumnName struct{ Column }
type UnmatchColumnValueType struct{ Column }

func (err NotFoundColumnName) Error() string {
	return "NotFoundColumnName: " + err.Name()
}

func (err UnmatchColumnValueType) Error() string {
	return "UnmatchColumnValueType: " + err.Name() + " " + columnTypeName(err)
}

var (
	WrongFileFormat = errors.New("WrongFileFormat")
	TooLargeData    = errors.New("TooLargeData")

	NotFoundData  = errors.New("NotFoundData")
	NotFoundTable = errors.New("NotFoundTable")
)

var (
	TableNameAlreadyExists = errors.New("TableNameAlreadyExists")
	UninitializedUnkoDB    = errors.New("UninitializedUnkoDB")
)

var (
	KeyAlreadyExists        = errors.New("KeyAlreadyExists")
	ColumnNameAlreadyExists = errors.New("ColumnNameAlreadyExists")
	ColumnNameIsTooLong     = errors.New("ColumnNameIsTooLong")
	NeedColumnName          = errors.New("NeedColumnName")
	InvalidOperation        = errors.New("InvalidOperation")
	NeedToSetAKey           = errors.New("NeedToSetAKey")
	SizeMustBePositiveValue = errors.New("SizeMustBePositiveValue")
	ColumnCountIsFull       = errors.New("ColumnCountIsFull")
)

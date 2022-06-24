// unkodb
// author: Leonardone @ NEETSDKASU

package unkodb

import "errors"

type NotFoundKeyName struct{ Column }
type UnmatchKeyValueType struct{ Column }

func (err NotFoundKeyName) Error() string {
	return "NotFoundKeyName: " + err.Name()
}

func (err UnmatchKeyValueType) Error() string {
	return "UnmatchKeyValueType: " + err.Name() + " " + columnTypeName(err)
}

var (
	WrongFileFormat = errors.New("WrongFileFormat")
	TooLargeData    = errors.New("TooLargeData")

	NotFoundData = errors.New("NotFoundData")
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

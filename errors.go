// unkodb
// author: Leonardone @ NEETSDKASU

package unkodb

import "errors"

var (
	notStruct = errors.New("notStruct")
)

type NotFoundColumnName struct{ Column }
type UnmatchColumnValueType struct{ Column }
type TagError struct{ inner error }
type FileFormatError struct { description string }

func (err NotFoundColumnName) Error() string {
	return "NotFoundColumnName: " + err.Name()
}

func (err UnmatchColumnValueType) Error() string {
	return "UnmatchColumnValueType: " + err.Name() + " " + ColumnTypeHint(err)
}

func (err TagError) Error() string {
	return "TagError: " + err.inner.Error()
}

func (err FileFormatError) Error() string {
    return "FileFormatError: " + err.description
}

var (
	WrongFileFormat = errors.New("WrongFileFormat")
	TooLargeData    = errors.New("TooLargeData")
	KeyIsNotCounter = errors.New("KeyIsNotCounter")

	NotFoundKey   = errors.New("NotFoundKey")
	NotFoundData  = errors.New("NotFoundData")
	NotFoundTable = errors.New("NotFoundTable")

	CannotAssignValueToField = errors.New("CannotAssignValueToField")
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

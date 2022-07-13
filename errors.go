// unkodb
// author: Leonardone @ NEETSDKASU

package unkodb

import "errors"

var (
	notStruct = errors.New("notStruct")
)

type NotFoundColumnName struct{ Column }

func (err NotFoundColumnName) Error() string {
	return "NotFoundColumnName: " + err.Name()
}

type UnmatchColumnValueType struct{ Column }

func (err UnmatchColumnValueType) Error() string {
	return "UnmatchColumnValueType: " + err.Name() + " " + ColumnTypeHint(err)
}

type TagError struct{ inner error }

func (err TagError) Error() string {
	return "TagError: " + err.inner.Error()
}

type WrongFileFormat struct{ description string }

func (err WrongFileFormat) Error() string {
	return "WrongFileFormat: " + err.description
}

var (
	TableNameIsTooLong = errors.New("TableNameIsTooLong")

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

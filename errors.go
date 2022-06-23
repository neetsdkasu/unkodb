// unkodb
// author: Leonardone @ NEETSDKASU

package unkodb

import "errors"

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
	InvalidOperation        = errors.New("InvalidOperation")
	NeedToSetAKey           = errors.New("NeedToSetAKey")
)

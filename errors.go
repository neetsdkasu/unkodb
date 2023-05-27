// unkodb
// author: Leonardone @ NEETSDKASU

package unkodb

import "errors"

var (
	errNotStruct = errors.New("errNotStruct")
)

// InsertやReplaceなどでテーブルのデータに必要なカラムが不足しているときのエラー
type ErrNotFoundColumnName struct{ Column }

func (err ErrNotFoundColumnName) Error() string {
	return "ErrNotFoundColumnName: " + err.Name()
}

// テーブルに定義されたカラム型に対応しないGoの型で値を受け取ったときのエラー
type ErrUnmatchColumnValueType struct{ Column }

func (err ErrUnmatchColumnValueType) Error() string {
	return "ErrUnmatchColumnValueType: " + err.Name() + " " + ColumnTypeHint(err)
}

// unkodbタグにおけるタグの記述に関するエラー
type TagError struct{ inner error }

func (err TagError) Error() string {
	return "TagError: " + err.inner.Error()
}

// 壊れているunkodbファイルあるいは無関係なファイルを読み込んだときのエラー
type ErrWrongFileFormat struct{ description string }

func (err ErrWrongFileFormat) Error() string {
	return "ErrWrongFileFormat: " + err.description
}

var (
	// テーブル名が長すぎるときのエラー
	ErrTableNameIsTooLong = errors.New("ErrTableNameIsTooLong")

	// String系やBytes系のカラム型に収まらないサイズのデータが渡されたときのエラー
	ErrTooLargeData = errors.New("ErrTooLargeData")

	// キーのカラム型がCounterのときに対応しないGoの型でデータが渡されたときのエラー
	ErrKeyIsNotCounter = errors.New("ErrKeyIsNotCounter")

	// FindやDeleteなどで存在しないキーが指定されたときのエラー
	ErrNotFoundKey = errors.New("ErrNotFoundKey")

	// InsertやReplaceなどで引数に受け付けない型を受け取ったときのエラー
	ErrNotFoundData = errors.New("ErrNotFoundData")

	// 存在しないテーブル名を指定されたときのエラー
	ErrNotFoundTable = errors.New("ErrNotFoundTable")

	// unkodbタグをつけたフィールドのGoの型が指定のカラム型に対応しないときのエラー
	ErrCannotAssignValueToField = errors.New("ErrCannotAssignValueToField")

	// 既に存在するテーブル名で新しくテーブルを作ろうとしたときのエラー
	TableNameAlreadyExists = errors.New("TableNameAlreadyExists")

	// テーブルでのInsertにおいて既に存在するキーでデータを追加しようとしたときのエラー
	// あるいは
	// テーブル作成時に２つめのキーを作成しようとしたときのエラー
	KeyAlreadyExists = errors.New("KeyAlreadyExists")

	// テーブルの作成時に同じカラム名のカラムを追加しようとしたときのエラー
	ColumnNameAlreadyExists = errors.New("ColumnNameAlreadyExists")

	// テーブルの作成時にカラム名が長すぎるときのエラー
	ColumnNameIsTooLong = errors.New("ColumnNameIsTooLong")

	// テーブル作成時に空のカラム名を設定しようとしたときのエラー
	NeedColumnName = errors.New("NeedColumnName")

	// 不正にメソッドを呼び出しされたときのエラー
	// 例えば、イテレーション中にテーブルを変更するメソッドを呼び出したときなど
	InvalidOperation = errors.New("InvalidOperation")

	// テーブル作成時にキーが設定されないままテーブルの生成処理が実行されたときのエラー
	NeedToSetAKey = errors.New("NeedToSetAKey")

	// テーブル作成時に固定長タイプのカラム型でサイズに0が指定されたときのエラー
	SizeMustBePositiveValue = errors.New("SizeMustBePositiveValue")

	// テーブル作成時にテーブルに設定できる最大カラム数を超えてカラムを作ろうとしたときのエラー
	ColumnCountIsFull = errors.New("ColumnCountIsFull")
)

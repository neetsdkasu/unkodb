// unkodb
// author: Leonardone @ NEETSDKASU

//
// ファイルフォーマット
// シグネチャ 16 bytes
//   0 1 2 3 4 5 6 7 8 9 'U' 'N' 'K' 'O' 'D' 'B'
package unkodb

import (
	"bytes"
	"io"
)

type File struct {
	file io.ReadWriteSeeker
}

type Segment struct {
	file     *File
	position int
	buffer   []byte
}

func Signature() []byte {
	return []byte{
		0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
		'U', 'N', 'K', 'O', 'D', 'B',
	}
}

func NewFile(file io.ReadWriteSeeker) (*File, error) {
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	sig := Signature()
	tmp := make([]byte, len(sig))
	if n, err := file.Read(tmp); err == nil {
		tmp = tmp[:n]
	} else {
		return nil, err
	}
	if bytes.Equal(tmp, sig) {

	} else if len(tmp) == 0 {
		// Empty File
	} else {

	}
	panic("TODO")
}

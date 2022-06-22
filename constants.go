// unkodb
// author: Leonardone @ NEETSDKASU

package unkodb

const (
	invalidColumnType ColumnType = iota
	Counter
	Int8
	Uint8
	Int16
	Uint16
	Int32
	Uint32
	Int64
	Uint64
	Float32
	Float64
	ShortString
	FixedSizeShortString
	LongString
	FixedSizeLongString
	Text
	ShortBytes
	FixedSizeShortBytes
	LongBytes
	FixedSizeLongBytes
	Blob
)

const (
	MaximumTableNameByteSize  = 255
	MaximumColumnNameByteSize = 255
	MaximumColumnCount        = 100
)

const (
	shortStringMinimumDataByteSize = 0
	shortStringMaximumDataByteSize = (1 << 8) - 1
	shortStringByteSizeDataLength  = 1

	shortBytesMinimumDataByteSize = 0
	shortBytesMaximumDataByteSize = (1 << 8) - 1
	shortBytesByteSizeDataLength  = 1

	longBytesMinimumDataByteSize = 0
	longBytesMaximumDataByteSize = (1 << 16) - 1
	longBytesByteSizeDataLength  = 2
)

const (
	tableListTableName  = "table_list"
	tableListKeyName    = "table_name"
	tableListColumnName = "columns_spec_buf"
)

const (
	maximumSegmentByteSize = (1 << 31) - 1
)

const (
	fileFormatVersion = 1

	addressByteSize = 4
	nullAddress     = 0

	fileHeaderSignaturePosition = 0
	fileHeaderSignatureLength   = 16

	fileHeaderFileFormatVersionPosition = fileHeaderSignaturePosition + fileHeaderSignatureLength
	fileHeaderFileFormatVersionLength   = 2

	fileHeaderNextNewSegmentAddressPosition = fileHeaderFileFormatVersionPosition + fileHeaderFileFormatVersionLength
	fileHeaderNextNewSegmentAddressLength   = addressByteSize

	fileHeaderReserveAreaAddressPosition = fileHeaderNextNewSegmentAddressPosition + fileHeaderNextNewSegmentAddressLength
	fileHeaderReserveAreaAddressLength   = addressByteSize

	fileHeaderTableListRootAddressPosition = fileHeaderReserveAreaAddressPosition + fileHeaderReserveAreaAddressLength
	fileHeaderTableListRootAddressLength   = addressByteSize

	fileHeaderIdleSegmentTreeRootAddressPosition = fileHeaderTableListRootAddressPosition + fileHeaderTableListRootAddressLength
	fileHeaderIdleSegmentTreeRootAddressLength   = addressByteSize

	fileHeaderByteSize = fileHeaderIdleSegmentTreeRootAddressPosition + fileHeaderIdleSegmentTreeRootAddressLength

	firstNewSegmentAddress = fileHeaderByteSize

	segmentHeaderByteSize = addressByteSize
)

// アホみたい
const (
	idleSegmentTreeNodeLeftChildPosition = 0
	idleSegmentTreeNodeLeftChildLength   = addressByteSize

	idleSegmentTreeNodeRightChildPosition = idleSegmentTreeNodeLeftChildPosition + idleSegmentTreeNodeLeftChildLength
	idleSegmentTreeNodeRightChildLength   = addressByteSize

	idleSegmentTreeNodeHeightPosition = idleSegmentTreeNodeRightChildPosition + idleSegmentTreeNodeRightChildLength
	idleSegmentTreeNodeHeightLength   = 1
)

// アホみたい
const (
	tableTreeNodeLeftChildPosition = 0
	tableTreeNodeLeftChildLength   = addressByteSize

	tableTreeNodeRightChildPosition = tableTreeNodeLeftChildPosition + tableTreeNodeLeftChildLength
	tableTreeNodeRightChildLength   = addressByteSize

	tableTreeNodeHeightPosition = tableTreeNodeRightChildPosition + tableTreeNodeRightChildLength
	tableTreeNodeHeightLength   = 1

	tableTreeNodeHeaderByteSize = tableTreeNodeHeightPosition + tableTreeNodeHeightLength
)

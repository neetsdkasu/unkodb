// unkodb
// author: Leonardone @ NEETSDKASU

package unkodb

const (
	structTagKey = "unkodb"
)

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
	MaximumTableNameByteSize     = 255
	MaximumColumnNameByteSize    = 255
	MaximumColumnCountWithoutKey = 100
)

const (
	shortStringMinimumDataByteSize = 0
	shortStringMaximumDataByteSize = (1 << 8) - 1 // 255
	shortStringByteSizeDataLength  = 1            // == unsafe.Sizeof(uint8(0))

	longStringMinimumDataByteSize = 0
	longStringMaximumDataByteSize = (1 << 16) - 1
	longStringByteSizeDataLength  = 2 // == unsafe.Sizeof(uint16(0))

	textMinimumDataByteSize = 0
	textMaximumDataByteSize = (1 << 30) - 1
	textByteSizeDataLength  = 4 // == unsafe.Sizeof(uint32(0))

	shortBytesMinimumDataByteSize = 0
	shortBytesMaximumDataByteSize = (1 << 8) - 1 // 255
	shortBytesByteSizeDataLength  = 1            // == unsafe.Sizeof(uint8(0))

	longBytesMinimumDataByteSize = 0
	longBytesMaximumDataByteSize = (1 << 16) - 1
	longBytesByteSizeDataLength  = 2 // == unsafe.Sizeof(uint16(0))

	blobMinimumDataByteSize = 0
	blobMaximumDataByteSize = (1 << 30) - 1
	blobByteSizeDataLength  = 4 // == unsafe.Sizeof(uint32(0))
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

	addressByteSize = 4 // == unsafe.Sizeof(int32(0))
	nullAddress     = 0

	fileHeaderSignaturePosition = 0
	fileHeaderSignatureLength   = 16

	fileHeaderFileFormatVersionPosition = fileHeaderSignaturePosition + fileHeaderSignatureLength
	fileHeaderFileFormatVersionLength   = 2 // == unsafe.Sizeof(uint16(0))

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
	idleSegmentTreeNodeHeightLength   = 1 // == unsafe.Sizeof(uint8(0))
)

// アホみたい
const (
	tableTreeNodeLeftChildPosition = 0
	tableTreeNodeLeftChildLength   = addressByteSize

	tableTreeNodeRightChildPosition = tableTreeNodeLeftChildPosition + tableTreeNodeLeftChildLength
	tableTreeNodeRightChildLength   = addressByteSize

	tableTreeNodeHeightPosition = tableTreeNodeRightChildPosition + tableTreeNodeRightChildLength
	tableTreeNodeHeightLength   = 1 // == unsafe.Sizeof(uint8(0))

	tableTreeNodeHeaderByteSize = tableTreeNodeHeightPosition + tableTreeNodeHeightLength
)

const (
	tableSpecRootAddressPosition = 0
	tableSpecRootAddressLength   = addressByteSize

	tableSpecNodeCountPosition = tableSpecRootAddressPosition + tableSpecRootAddressLength
	tableSpecNodeCountLength   = 4 // == unsafe.Sizeof(int32(0))

	tableSpecCounterPosition = tableSpecNodeCountPosition + tableSpecNodeCountLength
	tableSpecCounterLength   = 4 // == unsafe.Sizeof(uint32(0))

	tableSpecHeaderByteSize = tableSpecCounterPosition + tableSpecCounterLength
)

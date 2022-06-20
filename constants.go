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
	MaximumColumnNameByteSize = 255
)

const (
	shortStringMinimumDataByteSize = 0
	shortStringMaximumDataByteSize = 255
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

const (
	idleSegmentTreeNodeLeftChildPosition = 0
	idleSegmentTreeNodeLeftChildLength   = addressByteSize

	idleSegmentTreeNodeRightChildPosition = idleSegmentTreeNodeLeftChildPosition + idleSegmentTreeNodeLeftChildLength
	idleSegmentTreeNodeRightChildLength   = addressByteSize

	idleSegmentTreeNodeHeightPosition = idleSegmentTreeNodeRightChildPosition + idleSegmentTreeNodeRightChildLength
	idleSegmentTreeNodeHeightLength   = addressByteSize
)

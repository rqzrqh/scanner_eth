package types

type PublishOperation struct {
	BinlogRecordId uint64
	MessageId      uint64
	Height         uint64
	BinlogData     []byte
}

package types

type PublishFeedbackOperation struct {
	BinlogRecordId uint64
	MessageId      uint64
	Height         uint64
}

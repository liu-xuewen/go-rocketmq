package rocketmq

//SendStatus message send result
type SendStatus int

const (
	//SendOK message send success
	SendOK SendStatus = iota
	//FlushDiskTimeout FlushDiskTimeout
	FlushDiskTimeout
	//FlushSlaveTimeout FlushSlaveTimeout
	FlushSlaveTimeout
	//SlaveNotAvaliable SlaveNotAvaliable
	SlaveNotAvaliable
)

//SendResult rocketmq send result
type SendResult struct {
	sendStatus    SendStatus
	msgID         string
	messageQueue  MessageQueue
	queueOffset   int64
	transactionID string
	offsetMsgID   string
	regionID      string
	traceOn       bool
}

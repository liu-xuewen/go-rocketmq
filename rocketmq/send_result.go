package rocketmq

import "fmt"

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

func (s *SendResult) String() string {
	return fmt.Sprintf("sendStatus:%v, msgID:%v, topic:%v, queueId:%v, brokerName:%v",
		s.sendStatus, s.msgID, s.messageQueue.Topic, s.messageQueue.QueueId, s.messageQueue.BrokerName)
}

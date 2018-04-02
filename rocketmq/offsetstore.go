package rocketmq

import (
	"errors"
	"log"
	"sync"
	"sync/atomic"

	"github.com/powxiao/go-rocketmq/rocketmq/header"
)

const (
	MEMORY_FIRST_THEN_STORE = 0
	READ_FROM_MEMORY        = 1
	READ_FROM_STORE         = 2
)

type OffsetStore interface {
	updateOffset(mq *MessageQueue, offset int64, increaseOnly bool)
	readOffset(mq *MessageQueue, flag int) int64
}

// RemoteOffsetStore ...
type RemoteOffsetStore struct {
	groupName       string
	mqClient        *MqClient
	offsetTable     map[MessageQueue]int64
	offsetTableLock sync.RWMutex
}

func (self *RemoteOffsetStore) readOffset(mq *MessageQueue, readType int) int64 {
	switch readType {
	case MEMORY_FIRST_THEN_STORE:
	case READ_FROM_MEMORY:
		self.offsetTableLock.RLock()
		offset, ok := self.offsetTable[*mq]
		self.offsetTableLock.RUnlock()
		if ok {
			log.Printf("memory offset %v", offset)
			return offset
		} else if readType == READ_FROM_MEMORY {
			log.Println("memory offset -1")
			return -1
		}
	case READ_FROM_STORE:
		offset, err := self.fetchConsumeOffsetFromBroker(mq)

		if err != nil {
			log.Println(err)
			return -1
		}
		self.updateOffset(mq, offset, false)
		return offset
	}

	return -1

}

func (self *RemoteOffsetStore) fetchConsumeOffsetFromBroker(mq *MessageQueue) (int64, error) {
	brokerAddr, _, found := self.mqClient.findBrokerAddressInSubscribe(mq.BrokerName, 0, false)

	if !found {
		if err := self.mqClient.updateTopicRouteInfoFromNameServerByTopic(mq.Topic); err != nil {
			return 0, err
		}
		brokerAddr, _, found = self.mqClient.findBrokerAddressInSubscribe(mq.BrokerName, 0, false)
	}

	if found {
		requestHeader := &header.QueryConsumerOffsetRequestHeader{}
		requestHeader.Topic = mq.Topic
		requestHeader.QueueId = mq.QueueId
		requestHeader.ConsumerGroup = self.groupName
		return self.mqClient.queryConsumerOffset(brokerAddr, requestHeader, 3000)
	}

	return 0, errors.New("fetch consumer offset error")
}

func (self *RemoteOffsetStore) updateOffset(mq *MessageQueue, offset int64, increaseOnly bool) {
	if mq != nil {
		self.offsetTableLock.RLock()
		offsetOld, ok := self.offsetTable[*mq]
		self.offsetTableLock.RUnlock()
		if !ok {
			self.offsetTableLock.Lock()
			self.offsetTable[*mq] = offset
			self.offsetTableLock.Unlock()
		} else {
			if increaseOnly {
				atomic.AddInt64(&offsetOld, offset)
				self.offsetTableLock.Lock()
				self.offsetTable[*mq] = offsetOld
				self.offsetTableLock.Unlock()
			} else {
				self.offsetTableLock.Lock()
				self.offsetTable[*mq] = offset
				self.offsetTableLock.Unlock()
			}
		}
	}
}

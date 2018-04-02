package rocketmq

import (
	"errors"
	"log"
	"sort"
	"strings"
	"sync"
)

type SubscriptionData struct {
	Topic           string
	SubString       string
	ClassFilterMode bool
	TagsSet         []string
	CodeSet         []string
	SubVersion      int64
}

// Rebalance 用于选择MessageQueue
type Rebalance struct {
	groupName                    string
	messageModel                 string
	topicSubscribeInfoTable      map[string][]*MessageQueue
	topicSubscribeInfoTableLock  sync.RWMutex
	subscriptionInner            map[string]*SubscriptionData
	subscriptionInnerLock        sync.RWMutex
	mqClient                     *MqClient
	allocateMessageQueueStrategy AllocateMessageQueueStrategy
	consumer                     *DefaultConsumer
	processQueueTable            map[MessageQueue]int32
	processQueueTableLock        sync.RWMutex
	mutex                        sync.Mutex
}

func NewRebalance() *Rebalance {
	return &Rebalance{
		topicSubscribeInfoTable:      make(map[string][]*MessageQueue),
		subscriptionInner:            make(map[string]*SubscriptionData),
		allocateMessageQueueStrategy: new(AllocateMessageQueueAveragely),
		messageModel:                 "CLUSTERING", //BROADCASTING
		processQueueTable:            make(map[MessageQueue]int32),
	}
}

func (self *Rebalance) doRebalance() {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	for topic, _ := range self.subscriptionInner {
		self.rebalanceByTopic(topic)
	}
}

type ConsumerIdSorter []string

func (self ConsumerIdSorter) Len() int      { return len(self) }
func (self ConsumerIdSorter) Swap(i, j int) { self[i], self[j] = self[j], self[i] }
func (self ConsumerIdSorter) Less(i, j int) bool {
	if self[i] < self[j] {
		return true
	}
	return false
}

type AllocateMessageQueueStrategy interface {
	allocate(consumerGroup string, currentCID string, mqAll []*MessageQueue, cidAll []string) ([]*MessageQueue, error)
}
type AllocateMessageQueueAveragely struct{}

/*
	AllocateMessageQueueAveragely
	AllocateMessageQueueConsistentHash
*/
func (self *AllocateMessageQueueAveragely) allocate(consumerGroup string, currentCID string, mqAll []*MessageQueue, cidAll []string) ([]*MessageQueue, error) {
	if currentCID == "" {
		return nil, errors.New("currentCID is empty")
	}

	if mqAll == nil || len(mqAll) == 0 {
		return nil, errors.New("mqAll is nil or mqAll empty")
	}

	if cidAll == nil || len(cidAll) == 0 {
		return nil, errors.New("cidAll is nil or cidAll empty")
	}

	result := make([]*MessageQueue, 0)
	for i, cid := range cidAll {
		if cid == currentCID {
			mqLen := len(mqAll)
			cidLen := len(cidAll)
			mod := mqLen % cidLen
			var averageSize int
			if mqLen < cidLen {
				averageSize = 1
			} else {
				if mod > 0 && i < mod {
					averageSize = mqLen/cidLen + 1
				} else {
					averageSize = mqLen / cidLen
				}
			}

			var startIndex int
			if mod > 0 && i < mod {
				startIndex = i * averageSize
			} else {
				startIndex = i*averageSize + mod
			}

			var min int
			if averageSize > mqLen-startIndex {
				min = mqLen - startIndex
			} else {
				min = averageSize
			}

			for j := 0; j < min; j++ {
				result = append(result, mqAll[(startIndex+j)%mqLen])
			}
			return result, nil

		}
	}

	return nil, errors.New("cant't find currentCID")
}

func (self *Rebalance) rebalanceByTopic(topic string) error {
	switch self.messageModel {
	case "CLUSTERING":
		cidAll, err := self.mqClient.findConsumerIdList(topic, self.groupName)
		if err != nil {
			log.Printf("findConsumerIdList %v", err)
			return err
		}
		//log.Printf("findConsumerIdList successfully %+v", cidAll)

		self.topicSubscribeInfoTableLock.RLock()
		mqs, ok := self.topicSubscribeInfoTable[topic]
		self.topicSubscribeInfoTableLock.RUnlock()
		if ok && len(mqs) > 0 && len(cidAll) > 0 {
			var messageQueues MessageQueues = mqs
			var consumerIdSorter ConsumerIdSorter = cidAll

			sort.Sort(messageQueues)
			sort.Sort(consumerIdSorter)
		}

		//选择message queue
		allocateResult, err := self.allocateMessageQueueStrategy.allocate(self.groupName, self.mqClient.clientId, mqs, cidAll)

		if err != nil {
			log.Println(err)
			return err
		}
		self.updateProcessQueueTableInRebalance(topic, allocateResult)
		return nil
	default:
	}
	return nil
}

func (self *Rebalance) updateProcessQueueTableInRebalance(topic string, mqSet []*MessageQueue) {
	for _, mq := range mqSet {
		self.processQueueTableLock.RLock()
		_, ok := self.processQueueTable[*mq]
		self.processQueueTableLock.RUnlock()
		if !ok {
			pullRequest := new(PullRequest)
			pullRequest.consumerGroup = self.groupName
			pullRequest.messageQueue = mq
			pullRequest.nextOffset = self.computePullFromWhere(mq)
			self.consumer.pullRequestQueue <- pullRequest
			self.processQueueTableLock.Lock()
			self.processQueueTable[*mq] = 1
			self.processQueueTableLock.Unlock()
		}
	}

}

func (self *Rebalance) computePullFromWhere(mq *MessageQueue) int64 {
	var result int64 = -1
	lastOffset := self.consumer.offsetStore.readOffset(mq, READ_FROM_STORE)
	switch self.consumer.consumeFromWhere {
	case CONSUME_FROM_LAST_OFFSET:
		//log.Printf("CONSUME_FROM_LAST_OFFSET mq %v lastOffset %v", mq.QueueId, lastOffset)
		if lastOffset >= 0 {
			result = lastOffset
		} else {
			if strings.HasPrefix(mq.Topic, RETRY_GROUP_TOPIC_PREFIX) {
				result = 0
			} else {
				result = self.mqClient.getMaxOffset(mq)
			}
		}
		break
	case CONSUME_FROM_FIRST_OFFSET:
		//log.Printf("CONSUME_FROM_FIRST_OFFSET mq %v lastOffset %v", mq.QueueId, lastOffset)
		if lastOffset >= 0 {
			result = lastOffset
		} else {
			result = 0 // use the begin offset
		}
		break
	case CONSUME_FROM_TIMESTAMP:
		//log.Printf("CONSUME_FROM_TIMESTAMP mq %v lastOffset %v", mq.QueueId, lastOffset)
		if lastOffset >= 0 {
			result = lastOffset
		} else {
			if strings.HasPrefix(mq.Topic, RETRY_GROUP_TOPIC_PREFIX) {
				result = 0
			} else {
				result = self.mqClient.searchOffset(mq, self.consumer.consumeTimeStamp)
			}
		}
		break
	default:

	}

	return result
}

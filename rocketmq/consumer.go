package rocketmq

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"time"

	"log"

	"github.com/powxiao/go-rocketmq/rocketmq/header"
)

const (
	BrokerSuspendMaxTimeMillis       = 1000 * 15
	FLAG_COMMIT_OFFSET         int32 = 0x1 << 0
	FLAG_SUSPEND               int32 = 0x1 << 1
	FLAG_SUBSCRIPTION          int32 = 0x1 << 2
	FLAG_CLASS_FILTER          int32 = 0x1 << 3
)

type MessageListener func(msgs []*MessageExt) error

var DEFAULT_IP = GetLocalIp4()

type Config struct {
	NameServer   string
	ClientIp     string
	InstanceName string
}

/*
listener:
MessageListenerOrderly
MessageListenerConcurrently


DefaultMQPullConsumerImpl
		pullConsumer = new DefaultMQPullConsumer(consumerGroup);
        pullConsumer.setNamesrvAddr("127.0.0.1:9876");
        pullConsumer.start();
		findBrokerAddressInSubscribe
		PullMessageRequestHeader
		MessageQueue messageQueue = new MessageQueue(topic, brokerName, 0);
		pullConsumer.pull(messageQueue, "*", 1024, 3);
		pullConsumer.pull(messageQueue, "*", 1024, 3, new PullCallBack() {
		});



DefaultMQPushConsumerImpl
		pushConsumer = new DefaultMQPushConsumer(consumerGroup);
        pushConsumer.setNamesrvAddr("127.0.0.1:9876");
		pushConsumer.setPullInterval(60 * 1000);
        pushConsumer.registerMessageListener(new MessageListenerConcurrently() {
            ...
        });
		pushConsumer.set(rebalancePushImpl)
		pushConsumer.subscribe(topic, "*");
        pushConsumer.start();
		mQClientFactory.registerConsumer(consumerGroup, pushConsumerImpl);
		PullMessageRequestHeader
		createPullResult
		findBrokerAddressInSubscribe
		findConsumerIdList
		updateTopicSubscribeInfo
		ConsumeMessageConcurrentlyService
		pullMessageService.executePullRequestImmediately(createPullRequest())

RebalancePush topicSubscribeInfoTable
        RebalancePushImpl rebalancePush = new RebalancePushImpl(consumerGroup, MessageModel.CLUSTERING,
            new AllocateMessageQueueAveragely(), mqClientInstance, defaultMQPushConsumer);

ProcessQueue
	PutMessage/ TakeMessage/ RemoveMessage
	fillProcessQueueInfo


rebalanceByTopic
processQueueTable
findConsumerIdList
getConsumerIdListByGroup
pullMessage LOOP
*/
type Consumer interface {
	//Admin
	Start() error
	Shutdown()
	RegisterMessageListener(listener MessageListener)
	Subscribe(topic string, subExpression string)
	UnSubscribe(topic string)
	SendMessageBack(msg MessageExt, delayLevel int) error //brokerName
	fetchSubscribeMessageQueues(topic string) error
}

type DefaultConsumer struct {
	conf             *Config
	consumerGroup    string
	consumeFromWhere string
	consumerType     string
	messageModel     string
	unitMode         bool

	subscription    map[string]string
	messageListener MessageListener
	offsetStore     OffsetStore
	brokers         map[string]net.Conn

	rebalance      *Rebalance
	remotingClient RemotingClient
	mqClient       *MqClient
}

func NewDefaultConsumer(name string, conf *Config) (Consumer, error) {
	if conf == nil {
		conf = &Config{
			NameServer:   os.Getenv("ROCKETMQ_NAMESVR"),
			InstanceName: "DEFAULT",
		}
	}

	if conf.ClientIp == "" {
		conf.ClientIp = DEFAULT_IP
	}

	remotingClient := NewDefaultRemotingClient()
	mqClient := NewMqClient()

	rebalance := NewRebalance()
	rebalance.groupName = name
	rebalance.mqClient = mqClient

	offsetStore := new(RemoteOffsetStore)
	offsetStore.mqClient = mqClient
	offsetStore.groupName = name
	offsetStore.offsetTable = make(map[MessageQueue]int64)

	pullMessageService := NewPullMessageService()

	consumer := &DefaultConsumer{
		conf:             conf,
		consumerGroup:    name,
		consumeFromWhere: "CONSUME_FROM_LAST_OFFSET",
		subscription:     make(map[string]string),
		offsetStore:      offsetStore,
		brokers:          make(map[string]net.Conn),
		rebalance:        rebalance,
		remotingClient:   remotingClient,
		mqClient:         mqClient,
	}

	mqClient.consumerTable[name] = consumer
	mqClient.remotingClient = remotingClient
	mqClient.conf = conf
	mqClient.clientId = conf.ClientIp + "@" + strconv.Itoa(os.Getpid())
	mqClient.pullMessageService = pullMessageService

	rebalance.consumer = consumer
	pullMessageService.consumer = consumer

	return consumer, nil
}

func (self *DefaultConsumer) Start() error {
	self.mqClient.start()
	return nil
}

func (self *DefaultConsumer) Shutdown() {
}
func (self *DefaultConsumer) RegisterMessageListener(messageListener MessageListener) {
	self.messageListener = messageListener
}

func (self *DefaultConsumer) Subscribe(topic string, subExpression string) {
	self.subscription[topic] = subExpression

	subData := &SubscriptionData{
		Topic:     topic,
		SubString: subExpression,
	}
	self.rebalance.subscriptionInner[topic] = subData
}

func (self *DefaultConsumer) UnSubscribe(topic string) {
	delete(self.subscription, topic)
}

func (self *DefaultConsumer) SendMessageBack(msg MessageExt, delayLevel int) error {
	return nil
}

func (self *DefaultConsumer) fetchSubscribeMessageQueues(topic string) error {
	return nil
}

func (slef *DefaultConsumer) parseNextBeginOffset(responseCommand *RemotingCommand) (nextBeginOffset int64) {
	var err error
	pullResult := responseCommand.ExtFields
	if nextBeginOffsetInter, ok := pullResult["nextBeginOffset"]; ok {
		if nextBeginOffsetStr, ok := nextBeginOffsetInter.(string); ok {
			nextBeginOffset, err = strconv.ParseInt(nextBeginOffsetStr, 10, 64)
			if err != nil {
				panic(err)
			}
		}
	}
	return
}

func (self *DefaultConsumer) pullMessage(pullRequest *PullRequest) {
	commitOffsetEnable := false
	commitOffsetValue := int64(0)

	commitOffsetValue = self.offsetStore.readOffset(pullRequest.messageQueue, READ_FROM_MEMORY)
	if commitOffsetValue > 0 {
		commitOffsetEnable = true
	}

	var sysFlag int32 = 0
	if commitOffsetEnable {
		sysFlag |= FLAG_COMMIT_OFFSET
	}

	sysFlag |= FLAG_SUSPEND

	subscriptionData, ok := self.rebalance.subscriptionInner[pullRequest.messageQueue.topic]
	var subVersion int64
	var subString string
	if ok {
		subVersion = subscriptionData.SubVersion
		subString = subscriptionData.SubString

		sysFlag |= FLAG_SUBSCRIPTION
	}

	requestHeader := new(header.PullMessageRequestHeader)
	requestHeader.ConsumerGroup = pullRequest.consumerGroup
	requestHeader.Topic = pullRequest.messageQueue.topic
	requestHeader.QueueId = pullRequest.messageQueue.queueId
	requestHeader.QueueOffset = pullRequest.nextOffset

	requestHeader.SysFlag = sysFlag
	requestHeader.CommitOffset = commitOffsetValue
	requestHeader.SuspendTimeoutMillis = BrokerSuspendMaxTimeMillis

	if ok {
		requestHeader.SubVersion = subVersion
		requestHeader.Subscription = subString
	}

	pullCallback := func(responseFuture *ResponseFuture) {
		var nextBeginOffset int64 = pullRequest.nextOffset
		if responseFuture != nil {
			responseCommand := responseFuture.responseCommand
			if responseCommand.Code == SUCCESS && len(responseCommand.Body) > 0 {
				nextBeginOffset = self.parseNextBeginOffset(responseCommand)
				msgs := decodeMessage(responseFuture.responseCommand.Body)
				err := self.messageListener(msgs)
				if err != nil {
					log.Println(err)
					//TODO retry
				} else {
					self.offsetStore.updateOffset(pullRequest.messageQueue, nextBeginOffset, false)
				}
			} else if responseCommand.Code == PULL_NOT_FOUND {
			} else if responseCommand.Code == PULL_RETRY_IMMEDIATELY || responseCommand.Code == PULL_OFFSET_MOVED {
				log.Printf("pull message error,code=%d,request=%v", responseCommand.Code, requestHeader)
				nextBeginOffset = self.parseNextBeginOffset(responseCommand)
				//time.Sleep(1 * time.Second)
			} else {
				log.Println(fmt.Sprintf("pull message error,code=%d,body=%s", responseCommand.Code, string(responseCommand.Body)))
				log.Println(pullRequest.messageQueue)
				time.Sleep(1 * time.Second)
			}
		} else {
			log.Println("responseFuture is nil")
		}

		nextPullRequest := &PullRequest{
			consumerGroup: pullRequest.consumerGroup,
			nextOffset:    nextBeginOffset,
			messageQueue:  pullRequest.messageQueue,
		}
		//log.Printf("pullRequestQueue <- nextPullRequest")
		self.mqClient.pullMessageService.pullRequestQueue <- nextPullRequest
	}

	brokerAddr, _, found := self.mqClient.findBrokerAddressInSubscribe(pullRequest.messageQueue.brokerName, 0, false)
	if found {
		remotingCommand := NewRemotingCommand(PULL_MESSAGE, requestHeader)
		self.remotingClient.invokeAsync(brokerAddr, remotingCommand, 1000, pullCallback)
	}
}

func (self *DefaultConsumer) updateTopicSubscribeInfo(topic string, info []*MessageQueue) {
	if self.rebalance.subscriptionInner != nil {
		self.rebalance.subscriptionInnerLock.RLock()
		_, ok := self.rebalance.subscriptionInner[topic]
		self.rebalance.subscriptionInnerLock.RUnlock()
		if ok {
			self.rebalance.subscriptionInnerLock.Lock()
			self.rebalance.topicSubscribeInfoTable[topic] = info
			self.rebalance.subscriptionInnerLock.Unlock()
		}
	}
}

func (self *DefaultConsumer) subscriptions() []*SubscriptionData {
	subscriptions := make([]*SubscriptionData, 0)
	for _, subscription := range self.rebalance.subscriptionInner {
		subscriptions = append(subscriptions, subscription)
	}
	return subscriptions
}

func (self *DefaultConsumer) doRebalance() {
	self.rebalance.doRebalance()
}

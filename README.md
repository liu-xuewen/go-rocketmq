# rocketMQ client sdk

# import
import	""github.com/powxiao/go-rocketmq/rocketmq""

# example
```
var (
	nameServerAddress = "127.0.0.1:9876" //address split by ;  (for example 192.168.1.1:9876;192.168.1.2:9876)
	testTopic         = "GoLangRMQ"
	testProducerGroup = "GoLangProducer"
	testConsumerGroup = "GoLangConsumer"
	conf              = &rocketmq.Config{
		NameServer:   nameServerAddress,
		InstanceName: "DEFAULT",
	}
)
```

```
type publishCallBack struct{}

func (p *publishCallBack) OnSuccess(result *rocketmq.SendResult) {
	log.Printf("%v", result.String())
}

func producerSync() {
	var producer = rocketmq.NewDefaultProducer(testProducerGroup, conf)
	producer.Start()
	var message = &rocketmq.MessageExt{}
	message.Topic = testTopic
	for i := 0; i < 10; i++ {
		message.Body = []byte(fmt.Sprintf("%d", i))
		_, err := producer.Send(message)
		if err != nil {
			log.Printf("fail to send message %v", err)
		} else {
			log.Printf("send %v", i)
		}
		time.Sleep(time.Second)
	}
}
```
```
func producerOneWay() {
	var producer = rocketmq.NewDefaultProducer(testProducerGroup, conf)
	producer.Start()
	var message = &rocketmq.MessageExt{}
	message.Topic = testTopic
	for i := 0; i < 10; i++ {
		message.Body = []byte(fmt.Sprintf("%d", i))
		producer.SendOneWay(message)
	}
}
```

```
func producerAsync() {
	var producer = rocketmq.NewDefaultProducer(testProducerGroup, conf)
	producer.Start()
	var message = &rocketmq.MessageExt{}
	message.Topic = testTopic
	for i := 0; i < 10; i++ {
		message.Body = []byte(fmt.Sprintf("%d", i))
		producer.SendAsync(message, &publishCallBack{})
	}
}
```

```
func producerOrdered() {
	var producer = rocketmq.NewDefaultProducer(testProducerGroup, conf)
	producer.Start()
	var message = &rocketmq.MessageExt{}
	message.Topic = testTopic
	for i := 0; i < 20; i++ {
		orderId := i % 5
		str := fmt.Sprintf("KEY[%d]-ORDERID[%d]", i, orderId)
		message.Body = []byte(str)
		_, err := producer.SendOrderly(message, orderId)
		if err != nil {
			log.Printf("fail to send message %v", err)
		} else {
			log.Printf("send %v", str)
		}
	}
}
```

```
func main() {
	log.Printf("start main")

	consumer, err := rocketmq.NewDefaultConsumer(testConsumerGroup, conf)
	if err != nil {
		log.Println(err)
		return
	}
	consumer.Subscribe(testTopic, "*")
	consumer.RegisterMessageListener(func(msgs []*rocketmq.MessageExt) error {
		for _, msg := range msgs {
			log.Printf("[%s] msgBody[%s]", msg.MsgId, string(msg.Body))
		}
		return nil
	})
	consumer.Start()
	producerOneWay()
	s := make(chan os.Signal, 1)
	signal.Notify(s, syscall.SIGINT, syscall.SIGTERM)
	<-s
}
```

# feature plan
## producer
* createTopicRoute
* MessageQueueSelector
    - SelectMessageQueueByHash
    - SelectMessageQueueByRandom
* Sync
* Async
    - SendCallback
* OneWay
* Transaction

## consumer
* listener
    - MessageListenerOrderly
    - MessageListenerConcurrently
* PullConsumer
    - pull
* PushConsumer
    - subscribe
* ConsumeFromWhere
* ProcessQueue
    - cleanExpiredMsg
    - putMessage
    - removeMessage
    - takeMessages

## rebalance
* AllocateMessageQueueStrategy
    - AllocateMessageQueueAveragely
    - AllocateMessageQueueConsistentHash
    
## mqClient
* getTopicRouteInfoFromNameServer
* getTopicListFromNameServer
* getConsumerIdListByGroup
* sendMessage
* pullMessage 
* sendHearBeat

## remotingClient
* registerProcessor
* invokeSync
* invokeAsync
* invokeOneWay

## remotingCommand
* createRequestCommand
* createResponseCommand
* encodeHeader

## message
* messageQueue
* messageExt

## offsetStore
* LocalFileOffsetStore
* RemoteBrokerOffsetStore

## other
* batch
* broadcast
* filter
* orderMessage
* transaction




# rocketMQ client sdk

# import
import	"github.com/powxiao/rocketmq"

# example
* 见example.go

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




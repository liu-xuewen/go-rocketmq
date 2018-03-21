# rocketMQ client sdk

#import
import	"git.dz11.com/vega/rocketmq"

#example
* 见example.go
* consumer
* producer
* rebalance
* offsetStore
* remotingComand

#feature
## Producer
- [ ] ProducerType
    - [X] DefaultProducer
    - [ ] TransactionProducer
- [ ] API
    - [ ] Send
        - [X] Sync
        - [X] Async
        - [X] OneWay
- [ ] Other
    - [ ] DelayMessage
    - [ ] Config
    - [X] MessageId Generate
    - [ ] CompressMsg
    - [ ] TimeOut
    - [ ] LoadBalance
    - [ ] DefaultTopic
    - [ ] VipChannel
    - [ ] Retry
    - [ ] SendMessageHook
    - [ ] CheckRequestQueue
    - [ ] CheckForbiddenHookList
    - [ ] MQFaultStrategy

## Consumer
- [ ] ConsumerType
    - [ ] PushConsumer
    - [ ] PullConsumer
- [ ] MessageListener
    - [ ] Concurrently
    - [ ] Orderly
- [ ] MessageModel
    - [ ] CLUSTERING
    - [ ] BROADCASTING
- [ ] OffsetStore
    - [ ] RemoteBrokerOffsetStore
        - [ ] many actions
    - [ ] LocalFileOffsetStore
- [ ] RebalanceService
- [ ] PullMessageService
- [ ] ConsumeMessageService
- [ ] AllocateMessageQueueStrategy
    - [ ] AllocateMessageQueueAveragely
    - [ ] AllocateMessageQueueAveragelyByCircle
    - [ ] AllocateMessageQueueByConfig
    - [ ] AllocateMessageQueueByMachineRoom
- [ ] Other
    - [ ] Config
    - [ ] ZIP
    - [ ] AllocateMessageQueueStrategy
    - [ ] ConsumeFromWhere
        - [ ] CONSUME_FROM_LAST_OFFSET
        - [ ] CONSUME_FROM_FIRST_OFFSET
        - [ ] CONSUME_FROM_TIMESTAMP
    - [ ] Retry(sendMessageBack)
    - [ ] TimeOut(clearExpiredMessage)
    - [ ] ACK(partSuccess)
    - [ ] FlowControl(messageCanNotConsume)
    - [ ] ConsumeMessageHook
    - [ ] filterMessageHookList

## Remoting
- [ ] MqClientRequest
    - [ ] InvokeSync
    - [ ] InvokeAsync
    - [ ] InvokeOneWay
- [ ] Serialize
    - [ ] JSON
    - [ ] ROCKETMQ
- [ ] NamesrvAddrChoosed(HA)
- [ ] Other
    - [ ] VIPChannel
    - [ ] RPCHook

##Other
- [ ] Task
    - [ ] PollNameServer
    - [ ] Heartbeat
    - [ ] UpdateTopicRouteInfoFromNameServer
    - [ ] CleanOfflineBroker
    - [ ] PersistAllConsumerOffset
    - [ ] ClearExpiredMessage(form consumer consumeMessageService)
    - [ ] UploadFilterClassSource(FromHeartBeat/But Golang Not Easy To do this(Java Source))
- [ ] ClientRemotingProcessor
    - [ ] CHECK_TRANSACTION_STATE
    - [ ] NOTIFY_CONSUMER_IDS_CHANGED
    - [ ] RESET_CONSUMER_CLIENT_OFFSET
    - [ ] GET_CONSUMER_STATUS_FROM_CLIENT
    - [ ] GET_CONSUMER_RUNNING_INFO
    - [ ] CONSUME_MESSAGE_DIRECTLY

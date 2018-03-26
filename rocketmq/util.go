package rocketmq

import (
	"bytes"
	"compress/zlib"
	"log"
	"net"
	"strings"
	"time"
)

/*
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

## Other
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
*/

const (
	//MESSAGE_COMPRESS_LEVEL      MESSAGE_COMPRESS_LEVEL
	MESSAGE_COMPRESS_LEVEL = "rocketmq.message.compressLevel"
	//DEFAULT_TOPIC               DEFAULT_TOPIC
	DEFAULT_TOPIC = "TBW102"
	//CLIENT_INNER_PRODUCER_GROUP CLIENT_INNER_PRODUCER_GROUP
	CLIENT_INNER_PRODUCER_GROUP = "CLIENT_INNER_PRODUCER"

	//MASTER_ID MASTER_ID
	MASTER_ID int64 = 0
	//CURRENT_JVM_PID CURRENT_JVM_PID
	CURRENT_JVM_PID
	//RETRY_GROUP_TOPIC_PREFIX RETRY_GROUP_TOPIC_PREFIX
	RETRY_GROUP_TOPIC_PREFIX = "%RETRY%"
	//DLQ_GROUP_TOPIC_PREFIX     DLQ_GROUP_TOPIC_PREFIX
	DLQ_GROUP_TOPIC_PREFIX = "%DLQ%"
	//SYSTEM_TOPIC_PREFIX        SYSTEM_TOPIC_PREFIX
	SYSTEM_TOPIC_PREFIX = "rmq_sys_"
	//UNIQUE_MSG_QUERY_FLAG      UNIQUE_MSG_QUERY_FLAG
	UNIQUE_MSG_QUERY_FLAG = "_UNIQUE_KEY_QUERY"
	//MAX_MESSAGE_BODY_SIZE  MAX_MESSAGE_BODY_SIZE
	MAX_MESSAGE_BODY_SIZE int = 4 * 1024 * 1024 //4m
	//MAX_MESSAGE_TOPIC_SIZE MAX_MESSAGE_TOPIC_SIZE
	MAX_MESSAGE_TOPIC_SIZE int = 255 //255char
	//DEFAULT_TOPIC_QUEUE_NUMS DEFAULT_TOPIC_QUEUE_NUMS
	DEFAULT_TOPIC_QUEUE_NUMS int32 = 4
)

//ConsumeFromWhere consume from where
type ConsumeFromWhere int

const (
	//CONSUME_FROM_LAST_OFFSET first consume from the last offset
	CONSUME_FROM_LAST_OFFSET ConsumeFromWhere = iota

	//CONSUME_FROM_FIRST_OFFSET first consume from the first offset
	CONSUME_FROM_FIRST_OFFSET

	//CONSUME_FROM_TIMESTAMP first consume from the time
	CONSUME_FROM_TIMESTAMP
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Lmicroseconds)
	log.SetPrefix("[rocketMQ]")
}

//CompressWithLevel compress byte array with level
func CompressWithLevel(body []byte, level int) (compressBody []byte, err error) {
	var (
		in bytes.Buffer
		w  *zlib.Writer
	)
	w, err = zlib.NewWriterLevel(&in, level)
	if err != nil {
		return
	}
	_, err = w.Write(body)
	w.Close()
	compressBody = in.Bytes()
	return
}

//GetIp4Bytes get ip4 byte array
func GetIp4Bytes() (ret []byte) {
	ip := getIp()
	ret = ip[len(ip)-4:]
	return
}

//GetLocalIp4 get local ip4
func GetLocalIp4() (ip4 string) {
	ip := getIp()
	if ip.To4() != nil {
		currIp := ip.String()
		if !strings.Contains(currIp, ":") && currIp != "127.0.0.1" && isIntranetIpv4(currIp) {
			ip4 = currIp
		}
	}
	return
}

func getIp() (ip net.IP) {
	interfaces, err := net.Interfaces()
	if err != nil {
		return
	}

	for _, face := range interfaces {
		if strings.Contains(face.Name, "lo") {
			continue
		}
		addrs, err := face.Addrs()
		if err != nil {
			return
		}

		for _, addr := range addrs {
			if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
				if ipnet.IP.To4() != nil {
					currIp := ipnet.IP.String()
					if !strings.Contains(currIp, ":") && currIp != "127.0.0.1" && isIntranetIpv4(currIp) {
						ip = ipnet.IP
					}
				}
			}
		}
	}
	return
}

func isIntranetIpv4(ip string) bool {
	if strings.HasPrefix(ip, "192.168.") || strings.HasPrefix(ip, "169.254.") {
		return true
	}
	return false
}

func CurrentTimeMillisInt64() (ret int64) {
	ret = time.Now().UnixNano() / 1000000
	return
}

//NAME_VALUE_SEPARATOR char 1 and 2 from java code
var NAME_VALUE_SEPARATOR = string(rune(1))

//PROPERTY_SEPARATOR property separator
var PROPERTY_SEPARATOR = string(rune(2))

//MessageProperties2String convert message properties to string
func MessageProperties2String(propertiesMap map[string]string) (ret string) {
	for key, value := range propertiesMap {
		ret = ret + key + NAME_VALUE_SEPARATOR + value + PROPERTY_SEPARATOR
	}
	return
}

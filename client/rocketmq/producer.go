package rocketmq

import (
	"errors"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/StyleTang/incubator-rocketmq-externals/rocketmq-go/util"
	"github.com/apache/rocketmq-externals/rocketmq-go/model/constant"
	"github.com/apache/rocketmq-externals/rocketmq-go/remoting"
)

/*
1. 每次消息会发送失败或者超时，会重试（上限15次）
2.
*/

type SendCallBack interface {
	OnSuccess(result *SendResult)
}

//SendMessageRequestHeader of CustomerHeader
type SendMessageRequestHeader struct {
	ProducerGroup         string `json:"producerGroup"`
	Topic                 string `json:"topic"`
	DefaultTopic          string `json:"defaultTopic"`
	DefaultTopicQueueNums int    `json:"defaultTopicQueueNums"`
	QueueId               int32  `json:"queueId"`
	SysFlag               int    `json:"sysFlag"`
	BornTimestamp         int64  `json:"bornTimestamp"`
	Flag                  int32  `json:"flag"`
	Properties            string `json:"properties"`
	ReconsumeTimes        int    `json:"reconsumeTimes"`
	UnitMode              bool   `json:"unitMode"`
	MaxReconsumeTimes     int    `json:"maxReconsumeTimes"`
}

func (s *SendMessageRequestHeader) FromMap(headerMap map[string]interface{}) {
	return
}

/**
{
    "MSG_REGION": "DefaultRegion",
    "TRACE_ON": "true",
    "msgId": "C0A8000200002A9F0000000039FA93B5",
    "queueId": "3",
    "queueOffset": "1254671"
}
*/

//SendMessageResponseHeader of CustomerHeader
type SendMessageResponseHeader struct {
	MsgId         string
	QueueId       int32
	QueueOffset   int64
	TransactionId string
	MsgRegion     string
}

//FromMap convert map[string]interface to struct
func (s *SendMessageResponseHeader) FromMap(headerMap map[string]interface{}) {
	s.MsgId = headerMap["msgId"].(string)
	s.QueueId = util.StrToInt32WithDefaultValue(headerMap["queueId"].(string), -1)
	s.QueueOffset = util.StrToInt64WithDefaultValue(headerMap["queueOffset"].(string), -1)
	transactionId := headerMap["transactionId"]
	if transactionId != nil {
		s.TransactionId = headerMap["transactionId"].(string)
	}
	msgRegion := headerMap["MSG_REGION"]
	if msgRegion != nil {
		s.MsgRegion = headerMap["MSG_REGION"].(string)
	}
	return
}

type Producer interface {
	Start() error
	ShutDown()
	//send message,default timeout is 3000
	Send(msg *MessageExt) (sendResult *SendResult, err error)
	SendAsync(msg *MessageExt, sendCallBack SendCallBack)
	SendOneWay(msg *MessageExt)
}

// DefaultProducer ...
type DefaultProducer struct {
	conf              *Config
	producerGroup     string
	communicationMode string
	mqClient          *MqClient
}

// NewDefaultProducer ...
func NewDefaultProducer(name string, conf *Config) Producer {
	if conf == nil {
		conf = &Config{
			NameServer:   os.Getenv("ROCKETMQ_NAMESVR"),
			InstanceName: "DEFAULT",
		}
	}

	if conf.ClientIp == "" {
		conf.ClientIp = DEFAULT_IP
	}

	pullMessageService := NewPullMessageService()
	remotingClient := NewDefaultRemotingClient()
	mqClient := NewMqClient()
	producer := &DefaultProducer{
		producerGroup: name,
		mqClient:      mqClient,
	}

	mqClient.producerTable[name] = producer
	mqClient.remotingClient = remotingClient
	mqClient.conf = conf
	mqClient.clientId = conf.ClientIp + "@" + strconv.Itoa(os.Getpid())
	mqClient.pullMessageService = pullMessageService

	return producer
}

func (d *DefaultProducer) Start() error {
	d.mqClient.start()
	return nil
}

func (d *DefaultProducer) ShutDown() {

}

func (d *DefaultProducer) SendOneWay(msg *MessageExt) {

}

func (d *DefaultProducer) SendAsync(msg *MessageExt, sendCallback SendCallBack) {

}

func (d *DefaultProducer) Send(msg *MessageExt) (sendResult *SendResult, err error) {
	err = d.checkMessage(msg)
	if err != nil {
		return
	}
	var topicPublishInfo *TopicPublishInfo
	topicPublishInfo, err = d.mqClient.tryToFindTopicPublishInfo(msg.Topic)
	if err != nil {
		return
	}

	if topicPublishInfo.JudgeTopicPublishInfoOk() == false {
		err = errors.New("topicPublishInfo is error,topic=" + msg.Topic)
		return
	}

	var (
		lastFailedBroker string
		messageQueue     MessageQueue
	)

	communicationMode := "Sync" //默认是同步发送
	timeout := time.Second * 5  //默认发送超时时间为5s

	//retry to send message
	for times := 0; times < 15; times++ {
		messageQueue, err = selectOneMessageQueue(topicPublishInfo, lastFailedBroker) //没有选择ActiveMessageQueue,默认选第一个
		if err != nil {
			return
		}
		begin := time.Now()
		sendResult, err = d.doSendMessage(msg, messageQueue, communicationMode, nil, topicPublishInfo, int64(timeout))
		end := time.Now().Sub(begin)
		log.Printf("send message cost %#v(s)", end.Seconds())
		switch communicationMode {
		case "Async":
			return
		case "OneWay":
			return
		case "Sync":
			if sendResult.sendStatus != SendOK {
				continue
			}
			return
		default:
			break
		}
		if err != nil {
			return
		}
	}
	return
}

func (d *DefaultProducer) tryToCompressMessage(message *MessageExt) (compressedFlag int, err error) {
	if len(message.Body) < 1024*4 {
		compressedFlag = 0
		return
	}

	compressedFlag = int(CompressedFlag)
	var compressBody []byte
	compressBody, err = CompressWithLevel(message.Body, 5)
	message.Body = compressBody
	return
}

func (d *DefaultProducer) doSendMessage(msg *MessageExt, messageQueue MessageQueue, communicationMode string,
	sendCallback SendCallBack, info *TopicPublishInfo, timeout int64) (sendResult *SendResult, err error) {

	var (
		brokerAddr          string
		sysFlag             int
		compressMessageFlag int
	)
	sysFlag = sysFlag | compressMessageFlag
	compressMessageFlag, err = d.tryToCompressMessage(msg)
	if err != nil {
		return
	}
	msg.GeneratorMsgUniqueKey()
	sendMessageHeader := &SendMessageRequestHeader{
		ProducerGroup:         d.producerGroup,
		Topic:                 msg.Topic,
		DefaultTopic:          constant.DEFAULT_TOPIC,
		DefaultTopicQueueNums: 4,
		QueueId:               messageQueue.queueId,
		SysFlag:               sysFlag,
		BornTimestamp:         util.CurrentTimeMillisInt64(),
		Flag:                  msg.Flag,
		Properties:            util.MessageProperties2String(msg.Properties),
		UnitMode:              false,
		//ReconsumeTimes:        msg.GetReconsumeTimes(),
		//MaxReconsumeTimes:     msg.GetMaxReconsumeTimes(),
	}
	brokerAddr = d.mqClient.fetchMasterBrokerAddress(messageQueue.brokerName)
	if brokerAddr == "" {
		err = errors.New("The broker[" + messageQueue.brokerName + "] not exist")
		return
	}

	remoteClient := d.mqClient.remotingClient
	remotingCommand := NewRemotingCommandWithBody(remoting.SEND_MESSAGE, sendMessageHeader, msg.Body)

	switch communicationMode {
	case "Async":
		err = remoteClient.invokeAsync(brokerAddr, remotingCommand, timeout, func(responseFuture *ResponseFuture) {
			if sendCallback == nil && responseFuture.responseCommand != nil {
				sendResult, err = processSendResponse(messageQueue.brokerName, msg, responseFuture.responseCommand)
			}
			if responseFuture.responseCommand != nil {
				sendResult, err = processSendResponse(messageQueue.brokerName, msg, responseFuture.responseCommand)
				if sendCallback != nil {
					sendCallback.OnSuccess(sendResult)
				}
			} else {
			}
		})
		break
	case "Sync":
		var response *RemotingCommand
		response, err = remoteClient.invokeSync(brokerAddr, remotingCommand, timeout)
		if err != nil {
			log.Println(err)
			return
		}
		sendResult, err = processSendResponse(messageQueue.brokerName, msg, response)
		break
	case "OneWay":
		err = remoteClient.invokeOneWay(brokerAddr, remotingCommand, timeout)
		if err != nil {
			log.Println(err)
			return
		}
		break
	default:
		log.Printf("unknown producer communicate mode")
		break
	}
	if err != nil {
		log.Println(err)
		return
	}
	return
}

func processSendResponse(brokerName string, message *MessageExt,
	response *RemotingCommand) (sendResult *SendResult, err error) {
	sendResult = &SendResult{}
	switch response.Code {
	case remoting.FLUSH_DISK_TIMEOUT:
		{
			sendResult.sendStatus = FlushDiskTimeout
			break
		}
	case remoting.FLUSH_SLAVE_TIMEOUT:
		{
			sendResult.sendStatus = FlushSlaveTimeout
			break
		}
	case remoting.SLAVE_NOT_AVAILABLE:
		{
			sendResult.sendStatus = SlaveNotAvaliable
			break
		}
	case remoting.SUCCESS:
		{
			sendResult.sendStatus = SendOK
			break
		}
	default:
		err = errors.New("response.Code error_code=" + util.IntToString(int(response.Code)))
		return
	}
	var responseHeader = &SendMessageResponseHeader{}
	if response.ExtFields != nil {
		responseHeader.FromMap(response.ExtFields) //change map[string]interface{} into CustomerHeader struct
	}
	sendResult.msgID = message.Properties[PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX]
	sendResult.offsetMsgID = responseHeader.MsgId
	sendResult.queueOffset = responseHeader.QueueOffset
	sendResult.transactionID = responseHeader.TransactionId
	messageQueue := MessageQueue{topic: message.Topic, brokerName: brokerName,
		queueId: responseHeader.QueueId}
	sendResult.messageQueue = messageQueue
	var regionId = responseHeader.MsgRegion
	if len(regionId) == 0 {
		regionId = "DefaultRegion"
	}
	sendResult.regionID = regionId
	return
}

func (d *DefaultProducer) SendWithTimeout(msg MessageExt, timeout int64) (sendResult *SendResult, err error) {
	return nil, nil
}

func (d *DefaultProducer) checkMessage(msg *MessageExt) error {
	return nil
}

//if first select : random one
//if has error broker before ,skip the err broker
func selectOneMessageQueue(topicPublishInfo *TopicPublishInfo, lastFailedBroker string) (mqQueue MessageQueue, err error) {
	queueIndex := topicPublishInfo.FetchQueueIndex()
	queues := topicPublishInfo.MessageQueueList
	if len(lastFailedBroker) == 0 {
		mqQueue = queues[queueIndex]
		return
	}
	for i := 0; i < len(queues); i++ {
		nowQueueIndex := queueIndex + i
		if nowQueueIndex >= len(queues) {
			nowQueueIndex = nowQueueIndex - len(queues)
		}
		if lastFailedBroker == queues[nowQueueIndex].brokerName {
			continue
		}
		mqQueue = queues[nowQueueIndex]
		return
	}
	err = errors.New("send to [" + lastFailedBroker + "] fail,no other broker")
	return
}

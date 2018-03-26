package rocketmq

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/powxiao/go-rocketmq/rocketmq/header"
)

/*
sendMessageOneWay
SendResult sendResult = mqClientAPI.sendMessage(brokerAddr, brokerName, msg, new SendMessageRequestHeader(),
    3 * 1000, CommunicationMode.ONEWAY, new SendMessageContext(), defaultMQProducerImpl);

sendMessageAsync
*/

//DEFAULT_TIMEOUT rocketmq client's default timeout
var DEFAULT_TIMEOUT int64 = 3000

type QueueData struct {
	BrokerName     string
	ReadQueueNums  int32
	WriteQueueNums int32
	Perm           int32
	TopicSynFlag   int32
}

type BrokerData struct {
	BrokerName      string
	BrokerAddrs     map[string]string
	BrokerAddrsLock sync.RWMutex
}

type TopicRouteData struct {
	OrderTopicConf string
	QueueDatas     []*QueueData
	BrokerDatas    []*BrokerData
}

//TopicPublishInfo ...
type TopicPublishInfo struct {
	OrderTopic             bool
	HaveTopicRouterInfo    bool
	MessageQueueList       []MessageQueue
	TopicRouteDataInstance *TopicRouteData
	topicQueueIndex        int32
}

//JudgeTopicPublishInfoOk ...
func (t *TopicPublishInfo) JudgeTopicPublishInfoOk() (bIsTopicOk bool) {
	bIsTopicOk = len(t.MessageQueueList) > 0
	return
}

//FetchQueueIndex FetchQueueIndex
func (t *TopicPublishInfo) FetchQueueIndex() (index int) {
	qLen := len(t.MessageQueueList)
	if qLen > 0 {
		qIndex := atomic.AddInt32(&t.topicQueueIndex, 1)
		qIndex = qIndex % int32(qLen)
		index = int(qIndex)
	}
	return
}

type MqClient struct {
	clientId              string
	conf                  *Config
	brokerAddrTable       map[string]map[string]string //map[brokerName]map[bokerId]addrs
	brokerAddrTableLock   sync.RWMutex
	consumerTable         map[string]*DefaultConsumer
	consumerTableLock     sync.RWMutex
	producerTable         map[string]*DefaultProducer
	topicRouteTable       map[string]*TopicRouteData
	topicRouteTableLock   sync.RWMutex
	topicPublishInfoTable map[string]*TopicPublishInfo
	topicPublishInfoLock  sync.RWMutex
	remotingClient        RemotingClient
	pullMessageService    *PullMessageService
}

func NewMqClient() *MqClient {
	return &MqClient{
		brokerAddrTable:       make(map[string]map[string]string),
		consumerTable:         make(map[string]*DefaultConsumer),
		producerTable:         make(map[string]*DefaultProducer),
		topicRouteTable:       make(map[string]*TopicRouteData),
		topicPublishInfoTable: make(map[string]*TopicPublishInfo),
	}
}

const (
	PERM_PRIORITY = 0x1 << 3
	PERM_READ     = 0x1 << 2
	PERM_WRITE    = 0x1 << 1
	PERM_INHERIT  = 0x1 << 0
)

func WriteAble(perm int32) (ret bool) {
	ret = ((perm & PERM_WRITE) == PERM_WRITE)
	return
}
func ReadAble(perm int32) (ret bool) {
	ret = ((perm & PERM_READ) == PERM_READ)
	return
}

func BuildTopicSubscribeInfoFromRoteData(topic string, topicRouteData *TopicRouteData) (mqList []*MessageQueue) {
	mqList = make([]*MessageQueue, 0)
	for _, queueData := range topicRouteData.QueueDatas {
		if !ReadAble(queueData.Perm) {
			continue
		}
		var i int32
		for i = 0; i < queueData.ReadQueueNums; i++ {
			mq := &MessageQueue{
				topic:      topic,
				brokerName: queueData.BrokerName,
				queueId:    i,
			}
			mqList = append(mqList, mq)
		}
	}
	return
}

func BuildTopicPublishInfoFromTopicRoteData(topic string, topicRouteData *TopicRouteData) (topicPublishInfo *TopicPublishInfo) {
	// all order topic is false  todo change
	topicPublishInfo = &TopicPublishInfo{
		TopicRouteDataInstance: topicRouteData,
		OrderTopic:             false,
		MessageQueueList:       []MessageQueue{}}
	for _, queueData := range topicRouteData.QueueDatas {
		if !WriteAble(queueData.Perm) {
			continue
		}
		for _, brokerData := range topicRouteData.BrokerDatas {
			if brokerData.BrokerName == queueData.BrokerName {
				if len(brokerData.BrokerAddrs["0"]) == 0 {
					break
				}
				var i int32
				for i = 0; i < queueData.WriteQueueNums; i++ {
					messageQueue := MessageQueue{topic: topic, brokerName: queueData.BrokerName, queueId: i}
					topicPublishInfo.MessageQueueList = append(topicPublishInfo.MessageQueueList, messageQueue)
					topicPublishInfo.HaveTopicRouterInfo = true
				}
				break
			}
		}
	}
	return
}

func (m *MqClient) updateTopicRouteInfoLocal(topic string, topicRouteData *TopicRouteData) (err error) {
	if topicRouteData == nil {
		return
	}

	//update brokerAddrTable
	for _, brokerData := range topicRouteData.BrokerDatas {
		m.brokerAddrTableLock.Lock()
		m.brokerAddrTable[brokerData.BrokerName] = brokerData.BrokerAddrs
		m.brokerAddrTableLock.Unlock()
	}

	//update pubInfo for each
	topicPublishInfo := BuildTopicPublishInfoFromTopicRoteData(topic, topicRouteData)
	m.topicPublishInfoLock.Lock()
	m.topicPublishInfoTable[topic] = topicPublishInfo
	m.topicPublishInfoLock.Unlock()

	//mqList := BuildTopicSubscribeInfoFromRoteData(topic, topicRouteData)
	//m.topicSubscribeInfoTable.Set(topic, mqList)

	m.topicRouteTableLock.Lock()
	m.topicRouteTable[topic] = topicRouteData
	m.topicRouteTableLock.Unlock()
	return
}

func (m *MqClient) updateTopicRouteInfoFromNameServerUseDefaultTopic(topic string) (err error) {
	var (
		topicRouteData *TopicRouteData
	)
	//namesvr lock
	topicRouteData, err = m.getTopicRouteInfoFromNameServer(DEFAULT_TOPIC, DEFAULT_TIMEOUT)
	if err != nil {
		return
	}

	for _, queueData := range topicRouteData.QueueDatas {
		defaultQueueData := DEFAULT_TOPIC_QUEUE_NUMS
		if queueData.ReadQueueNums < defaultQueueData {
			defaultQueueData = queueData.ReadQueueNums
		}
		queueData.ReadQueueNums = defaultQueueData
		queueData.WriteQueueNums = defaultQueueData
	}
	m.updateTopicRouteInfoLocal(topic, topicRouteData)
	return
}

func (m *MqClient) tryToFindTopicPublishInfo(topic string) (topicPublicInfo *TopicPublishInfo, err error) {
	m.topicPublishInfoLock.RLock()
	value, ok := m.topicPublishInfoTable[topic]
	m.topicPublishInfoLock.RUnlock()
	if ok {
		topicPublicInfo = value
	}

	if topicPublicInfo == nil || !topicPublicInfo.JudgeTopicPublishInfoOk() {
		m.topicPublishInfoLock.Lock()
		m.topicPublishInfoTable[topic] = &TopicPublishInfo{HaveTopicRouterInfo: false}
		m.topicPublishInfoLock.Unlock()

		err = m.updateTopicRouteInfoFromNameServerByTopic(topic)
		if err != nil {
			log.Println(err) // if updateRouteInfo error, maybe we can use the defaultTopic
		}
		m.topicPublishInfoLock.RLock()
		value, ok := m.topicPublishInfoTable[topic]
		m.topicPublishInfoLock.RUnlock()

		if ok {
			topicPublicInfo = value
		}
	}
	if topicPublicInfo.HaveTopicRouterInfo && topicPublicInfo.JudgeTopicPublishInfoOk() {
		return
	}
	//try to use the defaultTopic
	err = m.updateTopicRouteInfoFromNameServerUseDefaultTopic(topic)

	m.topicPublishInfoLock.RLock()
	defaultValue, defaultValueOk := m.topicPublishInfoTable[topic]
	m.topicPublishInfoLock.RUnlock()

	if defaultValueOk {
		topicPublicInfo = defaultValue
	}
	return
}

func (self *MqClient) findBrokerAddressInSubscribe(brokerName string, brokerId int64, onlyThisBroker bool) (brokerAddr string, slave bool, found bool) {
	slave = false
	found = false
	self.brokerAddrTableLock.RLock()
	brokerMap, ok := self.brokerAddrTable[brokerName]
	self.brokerAddrTableLock.RUnlock()
	if ok {
		brokerAddr, ok = brokerMap[strconv.FormatInt(brokerId, 10)]
		slave = (brokerId != 0)
		found = ok

		if !found && !onlyThisBroker {
			var id string
			for id, brokerAddr = range brokerMap {
				slave = (id != "0")
				found = true
				break
			}
		}
	}

	return
}

func (self *MqClient) fetchMasterBrokerAddress(brokerName string) string {
	self.brokerAddrTableLock.RLock()
	brokers, ok := self.brokerAddrTable[brokerName]
	self.brokerAddrTableLock.RUnlock()
	if ok {
		for brokerId, addr := range brokers {
			if addr != "" && brokerId == "0" {
				return addr
			}
		}
	}
	return ""
}

func (self *MqClient) findBrokerAddrByTopic(topic string) (addr string, ok bool) {
	self.topicRouteTableLock.RLock()
	topicRouteData, ok := self.topicRouteTable[topic]
	self.topicRouteTableLock.RUnlock()
	if !ok {
		return "", ok
	}

	brokers := topicRouteData.BrokerDatas
	if brokers != nil && len(brokers) > 0 {
		brokerData := brokers[0]
		if ok {
			brokerData.BrokerAddrsLock.RLock()
			addr, ok = brokerData.BrokerAddrs["0"]
			brokerData.BrokerAddrsLock.RUnlock()

			if ok {
				return
			}
			for _, addr = range brokerData.BrokerAddrs {
				return addr, ok
			}
		}
	}
	return
}
func (self *MqClient) findConsumerIdList(topic string, groupName string) ([]string, error) {
	brokerAddr, ok := self.findBrokerAddrByTopic(topic)
	if !ok {
		err := self.updateTopicRouteInfoFromNameServerByTopic(topic)
		if err != nil {
			return nil, err
		}
		brokerAddr, ok = self.findBrokerAddrByTopic(topic)
	}

	if ok {
		return self.getConsumerIdListByGroup(brokerAddr, groupName, 3000)
	}

	return nil, errors.New("can't find broker")

}

func (self *MqClient) getConsumerIdListByGroup(addr string, consumerGroup string, timeoutMillis int64) ([]string, error) {
	requestHeader := new(header.GetConsumerListByGroupRequestHeader)
	requestHeader.ConsumerGroup = consumerGroup
	request := NewRemotingCommand(GET_CONSUMER_LIST_BY_GROUP, requestHeader)
	response, err := self.remotingClient.invokeSync(addr, request, timeoutMillis)
	if err != nil {
		log.Println(err)
		return nil, err
	}

	if response.Code == SUCCESS {
		getConsumerListByGroupResponseBody := new(header.GetConsumerListByGroupResponseBody)
		bodyjson := strings.Replace(string(response.Body), "0:", "\"0\":", -1)
		bodyjson = strings.Replace(bodyjson, "1:", "\"1\":", -1)
		err := json.Unmarshal([]byte(bodyjson), getConsumerListByGroupResponseBody)
		if err != nil {
			log.Println(err)
			return nil, err
		}
		return getConsumerListByGroupResponseBody.ConsumerIdList, nil
	}
	return nil, errors.New(fmt.Sprintf("fail to get ConsumerIdListByGroup, response code %v", response.Code))
}

func (self *MqClient) getTopicRouteInfoFromNameServer(topic string, timeoutMillis int64) (*TopicRouteData, error) {
	requestHeader := &header.GetRouteInfoRequestHeader{
		Topic: topic,
	}
	var remotingCommand = NewRemotingCommand(GET_ROUTEINTO_BY_TOPIC, requestHeader)
	response, err := self.remotingClient.invokeSync(self.conf.NameServer, remotingCommand, timeoutMillis)
	if err != nil {
		return nil, err
	}
	if response.Code == SUCCESS {
		topicRouteData := new(TopicRouteData)
		bodyjson := strings.Replace(string(response.Body), ",0:", ",\"0\":", -1)
		bodyjson = strings.Replace(bodyjson, ",1:", ",\"1\":", -1)
		bodyjson = strings.Replace(bodyjson, "{0:", "{\"0\":", -1)
		bodyjson = strings.Replace(bodyjson, "{1:", "{\"1\":", -1)
		err = json.Unmarshal([]byte(bodyjson), topicRouteData)
		if err != nil {
			log.Println(err)
			return nil, err
		}
		return topicRouteData, nil
	} else {
		return nil, errors.New(fmt.Sprintf("get topicRouteInfo from nameServer error[code:%d,topic:%s]", response.Code, topic))
	}

}

func (self *MqClient) updateTopicRouteInfoFromNameServer() {
	for _, consumer := range self.consumerTable {
		subscriptions := consumer.subscriptions()
		for _, subData := range subscriptions {
			self.updateTopicRouteInfoFromNameServerByTopic(subData.Topic)
		}
	}
}

func (self *MqClient) updateTopicRouteInfoFromNameServerByTopic(topic string) error {
	topicRouteData, err := self.getTopicRouteInfoFromNameServer(topic, 3000*1000)
	if err != nil {
		log.Println(err)
		return err
	}

	for _, bd := range topicRouteData.BrokerDatas {
		self.brokerAddrTableLock.Lock()
		self.brokerAddrTable[bd.BrokerName] = bd.BrokerAddrs
		self.brokerAddrTableLock.Unlock()
	}

	mqList := make([]*MessageQueue, 0)
	for _, queueData := range topicRouteData.QueueDatas {
		var i int32
		for i = 0; i < queueData.ReadQueueNums; i++ {
			mq := &MessageQueue{
				topic:      topic,
				brokerName: queueData.BrokerName,
				queueId:    i,
			}

			mqList = append(mqList, mq)
		}
	}

	for _, consumer := range self.consumerTable {
		consumer.updateTopicSubscribeInfo(topic, mqList)
	}
	self.topicRouteTableLock.Lock()
	self.topicRouteTable[topic] = topicRouteData
	self.topicRouteTableLock.Unlock()

	return nil
}

type ConsumerData struct {
	GroupName           string
	ConsumerType        string
	MessageModel        string
	ConsumeFromWhere    string
	SubscriptionDataSet []*SubscriptionData
	UnitMode            bool
}

type ProducerData struct {
	GroupName string
}

type HeartbeatData struct {
	ClientId        string
	ConsumerDataSet []*ConsumerData
	ProducerDataSet []*ProducerData
}

func (self *MqClient) prepareHeartbeatData() *HeartbeatData {
	heartbeatData := new(HeartbeatData)
	heartbeatData.ClientId = self.clientId
	heartbeatData.ConsumerDataSet = make([]*ConsumerData, 0)
	heartbeatData.ProducerDataSet = make([]*ProducerData, 0)
	for group, consumer := range self.consumerTable {
		consumerData := new(ConsumerData)
		consumerData.GroupName = group
		consumerData.ConsumerType = consumer.consumerType
		consumerData.ConsumeFromWhere = consumer.consumeFromWhere
		consumerData.MessageModel = consumer.messageModel
		consumerData.SubscriptionDataSet = consumer.subscriptions()
		consumerData.UnitMode = consumer.unitMode

		heartbeatData.ConsumerDataSet = append(heartbeatData.ConsumerDataSet, consumerData)
	}
	for group := range self.producerTable {
		producerData := new(ProducerData)
		producerData.GroupName = group
		heartbeatData.ProducerDataSet = append(heartbeatData.ProducerDataSet, producerData)
	}
	return heartbeatData
}

func (self *MqClient) sendHeartbeatToAllBrokerWithLock() error {
	heartbeatData := self.prepareHeartbeatData()
	if len(heartbeatData.ConsumerDataSet) == 0 {
		return errors.New("send heartbeat error")
	}

	self.brokerAddrTableLock.RLock()
	for _, brokerTable := range self.brokerAddrTable {
		for brokerId, addr := range brokerTable {
			if addr == "" || brokerId != "0" {
				continue
			}
			currOpaque := atomic.AddInt32(&opaque, 1)
			remotingCommand := &RemotingCommand{
				Code:     HEART_BEAT,
				Language: "JAVA",
				Version:  79,
				Opaque:   currOpaque,
				Flag:     0,
			}

			data, err := json.Marshal(*heartbeatData)
			if err != nil {
				log.Println(err)
				return err
			}
			remotingCommand.Body = data
			//log.Println("send heartbeat to broker[", addr+"]")
			response, err := self.remotingClient.invokeSync(addr, remotingCommand, 3000)
			if err != nil {
				log.Println(err)
			} else {
				if response == nil || response.Code != SUCCESS {
					log.Println("send heartbeat response  error")
				}
			}
		}
	}
	self.brokerAddrTableLock.RUnlock()
	return nil
}

func (self *MqClient) startScheduledTask() {
	go func() {
		updateTopicRouteTimer := time.NewTimer(5 * time.Second)
		for {
			<-updateTopicRouteTimer.C
			self.updateTopicRouteInfoFromNameServer()
			updateTopicRouteTimer.Reset(5 * time.Second)
		}
	}()

	go func() {
		heartbeatTimer := time.NewTimer(10 * time.Second)
		for {
			<-heartbeatTimer.C
			self.sendHeartbeatToAllBrokerWithLock()
			heartbeatTimer.Reset(5 * time.Second)
		}
	}()

	go func() {
		rebalanceTimer := time.NewTimer(15 * time.Second)
		for {
			<-rebalanceTimer.C
			self.doRebalance()
			rebalanceTimer.Reset(30 * time.Second)
		}
	}()

	go func() {
		timeoutTimer := time.NewTimer(3 * time.Second)
		for {
			<-timeoutTimer.C
			self.remotingClient.ScanResponseTable()
			timeoutTimer.Reset(time.Second)
		}
	}()

}

func (self *MqClient) doRebalance() {
	for _, consumer := range self.consumerTable {
		consumer.doRebalance()
	}
}

func (self *MqClient) start() {
	self.startScheduledTask()
	go self.pullMessageService.start()
}

type QueryConsumerOffsetRequestHeader struct {
	ConsumerGroup string `json:"consumerGroup"`
	Topic         string `json:"topic"`
	QueueId       int32  `json:"queueId"`
}

func (self *MqClient) queryConsumerOffset(addr string, requestHeader *QueryConsumerOffsetRequestHeader, timeoutMillis int64) (int64, error) {
	currOpaque := atomic.AddInt32(&opaque, 1)
	remotingCommand := &RemotingCommand{
		Code:     QUERY_CONSUMER_OFFSET,
		Language: "JAVA",
		Version:  79,
		Opaque:   currOpaque,
		Flag:     0,
	}

	remotingCommand.ExtFields = Struct2Map(requestHeader)
	reponse, err := self.remotingClient.invokeSync(addr, remotingCommand, timeoutMillis)

	if err != nil {
		log.Println(err)
		return 0, err
	}

	if reponse.Code == QUERY_NOT_FOUND {
		return 0, nil
	}

	extFields := reponse.ExtFields
	if offsetInter, ok := extFields["offset"]; ok {
		if offsetStr, ok := offsetInter.(string); ok {
			offset, err := strconv.ParseInt(offsetStr, 10, 64)
			if err != nil {
				log.Println(err)
				return 0, err
			}
			return offset, nil

		}
	}
	//log.Println(requestHeader, reponse)
	return 0, errors.New("query offset error")
}

func (self *MqClient) updateConsumerOffsetOneway(addr string, header *UpdateConsumerOffsetRequestHeader, timeoutMillis int64) {

	currOpaque := atomic.AddInt32(&opaque, 1)
	remotingCommand := &RemotingCommand{
		Code:      QUERY_CONSUMER_OFFSET,
		Language:  "JAVA",
		Version:   79,
		Opaque:    currOpaque,
		Flag:      0,
		ExtFields: Struct2Map(header),
	}

	self.remotingClient.invokeSync(addr, remotingCommand, timeoutMillis)
}

package rocketmq

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"encoding/json"
	"io/ioutil"
	"log"
)

const (
	CompressedFlag = (0x1 << 0)
)

const (
	//PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX
	PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX = "UNIQ_KEY"
)

type Message struct {
	Topic      string
	Flag       int32
	Properties map[string]string
	Body       []byte
}

type MessageExt struct {
	Message
	QueueId       int32
	StoreSize     int32
	QueueOffset   int64
	SysFlag       int32
	BornTimestamp int64
	//bornHost
	StoreTimestamp int64
	//storeHost
	MsgId                     string
	CommitLogOffset           int64
	BodyCRC                   int32
	ReconsumeTimes            int32
	PreparedTransactionOffset int64
}

//SetProperties set message's Properties
func (m *MessageExt) SetProperties(properties map[string]string) {
	m.Properties = properties
	return
}

//SetBody set body
func (m *MessageExt) SetBody(body []byte) {
	m.Body = body
}

//SetFlag set message flag
func (m *MessageExt) SetFlag(flag int32) {
	m.Flag = flag
	return
}

//SetTopic set topic
func (m *MessageExt) SetTopic(topic string) {
	m.Topic = topic
}

//GetMsgUniqueKey only use by system
func (m *MessageExt) GetMsgUniqueKey() string {
	if m.Properties != nil {
		originMessageId := m.Properties[PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX]
		if len(originMessageId) > 0 {
			return originMessageId
		}
	}
	return m.MsgId
}

//GeneratorMsgUniqueKey only use by system
func (m *MessageExt) GeneratorMsgUniqueKey() {
	if m.Properties == nil {
		m.Properties = make(map[string]string)
	}
	if len(m.Properties[PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX]) > 0 {
		return
	}
	m.Properties[PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX] = generatorMessageClientId()
}

func decodeMessage(data []byte) []*MessageExt {
	buf := bytes.NewBuffer(data)
	var storeSize, magicCode, bodyCRC, queueId, flag, sysFlag, reconsumeTimes, bodyLength, bornPort, storePort int32
	var queueOffset, physicOffset, preparedTransactionOffset, bornTimeStamp, storeTimestamp int64
	var topicLen byte
	var topic, body, properties, bornHost, storeHost []byte
	var propertiesLength int16

	var propertiesmap map[string]string

	msgs := make([]*MessageExt, 0, 32)
	for buf.Len() > 0 {
		msg := new(MessageExt)
		binary.Read(buf, binary.BigEndian, &storeSize)
		binary.Read(buf, binary.BigEndian, &magicCode)
		binary.Read(buf, binary.BigEndian, &bodyCRC)
		binary.Read(buf, binary.BigEndian, &queueId)
		binary.Read(buf, binary.BigEndian, &flag)
		binary.Read(buf, binary.BigEndian, &queueOffset)
		binary.Read(buf, binary.BigEndian, &physicOffset)
		binary.Read(buf, binary.BigEndian, &sysFlag)
		binary.Read(buf, binary.BigEndian, &bornTimeStamp)
		bornHost = make([]byte, 4)
		binary.Read(buf, binary.BigEndian, &bornHost)
		binary.Read(buf, binary.BigEndian, &bornPort)
		binary.Read(buf, binary.BigEndian, &storeTimestamp)
		storeHost = make([]byte, 4)
		binary.Read(buf, binary.BigEndian, &storeHost)
		binary.Read(buf, binary.BigEndian, &storePort)
		binary.Read(buf, binary.BigEndian, &reconsumeTimes)
		binary.Read(buf, binary.BigEndian, &preparedTransactionOffset)
		binary.Read(buf, binary.BigEndian, &bodyLength)
		if bodyLength > 0 {
			body = make([]byte, bodyLength)
			binary.Read(buf, binary.BigEndian, body)

			if (sysFlag & CompressedFlag) == CompressedFlag {
				b := bytes.NewReader(body)
				z, err := zlib.NewReader(b)
				if err != nil {
					log.Println(err)
					return nil
				}
				defer z.Close()
				body, err = ioutil.ReadAll(z)
				if err != nil {
					log.Println(err)
					return nil
				}
			}

		}
		binary.Read(buf, binary.BigEndian, &topicLen)
		topic = make([]byte, topicLen)
		binary.Read(buf, binary.BigEndian, &topic)
		binary.Read(buf, binary.BigEndian, &propertiesLength)
		if propertiesLength > 0 {
			properties = make([]byte, propertiesLength)
			binary.Read(buf, binary.BigEndian, &properties)
			propertiesmap = make(map[string]string)
			json.Unmarshal(properties, &propertiesmap)
		}

		if magicCode != -626843481 {
			log.Printf("magic code is error %d", magicCode)
			return nil
		}

		msg.SetTopic(string(topic))
		msg.QueueId = queueId
		msg.SysFlag = sysFlag
		msg.QueueOffset = queueOffset
		msg.BodyCRC = bodyCRC
		msg.StoreSize = storeSize
		msg.BornTimestamp = bornTimeStamp
		msg.ReconsumeTimes = reconsumeTimes
		msg.SetFlag(int32(flag))
		msg.CommitLogOffset = physicOffset
		msg.StoreTimestamp = storeTimestamp
		msg.PreparedTransactionOffset = preparedTransactionOffset
		msg.SetBody(body)
		msg.SetProperties(propertiesmap)
		//  <  3.5.8 use messageOffsetId
		//  >= 3.5.8 use clientUniqMsgId
		msg.MsgId = msg.GetMsgUniqueKey()
		if msg.MsgId == "" {
			msg.MsgId = GeneratorMessageOffsetId(storeHost, storePort, msg.CommitLogOffset)
		}
		msgs = append(msgs, msg)
	}

	return msgs
}

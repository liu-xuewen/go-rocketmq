package rocketmq

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"log"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
)

const (
	RPC_TYPE      int = 0 // 0, REQUEST_COMMAND
	RPC_ONEWAYint     = 1 // 0, RPC
)

var opaque int32
var decodeLock sync.Mutex

var (
	remotingVersionKey string = "rocketmq.remoting.version"
	ConfigVersion      int    = -1
	requestId          int32  = 0
)

//REMOTING_COMMAND_FLAG  0, REQUEST_COMMAND
var REMOTING_COMMAND_FLAG = 0

//REMOTING_COMMAND_LANGUAGE org.apache.rocketmq.remoting.protocol.LanguageCode
var REMOTING_COMMAND_LANGUAGE = "OTHER"

//REMOTING_COMMAND_VERSION org.apache.rocketmq.common.MQVersion.Version.ordinal()
var REMOTING_COMMAND_VERSION int16 = 213

type RemotingCommand struct {
	//header
	Code      int16                  `json:"code"`
	Language  string                 `json:"language"`
	Version   int16                  `json:"version"`
	Opaque    int32                  `json:"opaque"`
	Flag      int                    `json:"flag"`
	Remark    string                 `json:"remark"`
	ExtFields map[string]interface{} `json:"extFields"`
	//body
	Body []byte `json:"body,omitempty"`
}

func (self *RemotingCommand) encodeHeader() []byte {
	length := 4
	headerData := self.buildHeader()
	length += len(headerData)

	if self.Body != nil {
		length += len(self.Body)
	}

	buf := bytes.NewBuffer([]byte{})
	binary.Write(buf, binary.BigEndian, length)
	binary.Write(buf, binary.BigEndian, len(self.Body))
	buf.Write(headerData)

	return buf.Bytes()
}

func (self *RemotingCommand) buildHeader() []byte {
	buf, err := json.Marshal(self)
	if err != nil {
		return nil
	}
	return buf
}

func (self *RemotingCommand) encode() []byte {
	length := 4

	headerData := self.buildHeader()
	length += len(headerData)

	if self.Body != nil {
		length += len(self.Body)
	}

	buf := bytes.NewBuffer([]byte{})
	binary.Write(buf, binary.LittleEndian, length)
	binary.Write(buf, binary.LittleEndian, len(self.Body))
	buf.Write(headerData)

	if self.Body != nil {
		buf.Write(self.Body)
	}

	return buf.Bytes()
}

type CustomerHeader interface {
	//FromMap convert map[string]interface to struct
	FromMap(headerMap map[string]interface{})
}

//NewRemotingCommand NewRemotingCommand
func NewRemotingCommand(commandCode int16, customerHeader CustomerHeader) *RemotingCommand {
	return NewRemotingCommandWithBody(commandCode, customerHeader, nil)
}

//NewRemotingCommandWithBody NewRemotingCommandWithBody
func NewRemotingCommandWithBody(commandCode int16, customerHeader CustomerHeader, body []byte) *RemotingCommand {
	remotingCommand := new(RemotingCommand)
	remotingCommand.Code = commandCode
	currOpaque := atomic.AddInt32(&opaque, 1)
	remotingCommand.Opaque = currOpaque
	remotingCommand.Flag = REMOTING_COMMAND_FLAG
	remotingCommand.Language = REMOTING_COMMAND_LANGUAGE
	remotingCommand.Version = REMOTING_COMMAND_VERSION
	if customerHeader != nil {
		remotingCommand.ExtFields = Struct2Map(customerHeader)
	}
	remotingCommand.Body = body
	return remotingCommand
}

func Struct2Map(structBody interface{}) (resultMap map[string]interface{}) {
	resultMap = make(map[string]interface{})
	value := reflect.ValueOf(structBody)
	for value.Kind() == reflect.Ptr {
		value = value.Elem()
	}
	if value.Kind() != reflect.Struct {
		panic("input is not a struct")
	}
	valueType := value.Type()
	for i := 0; i < valueType.NumField(); i++ {
		field := valueType.Field(i)
		if field.PkgPath != "" {
			continue
		}
		name := field.Name
		smallName := strings.Replace(name, string(name[0]), string(strings.ToLower(string(name[0]))), 1)
		val := value.FieldByName(name).Interface()
		resultMap[smallName] = val
	}
	return
}

func decodeRemoteCommand(header, body []byte) *RemotingCommand {
	decodeLock.Lock()
	defer decodeLock.Unlock()

	cmd := &RemotingCommand{}
	cmd.ExtFields = make(map[string]interface{})
	err := json.Unmarshal(header, cmd)
	if err != nil {
		log.Print(err)
		return nil
	}
	cmd.Body = body
	return cmd
}

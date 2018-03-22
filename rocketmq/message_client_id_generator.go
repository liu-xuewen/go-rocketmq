package rocketmq

import (
	"bytes"
	"encoding/binary"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	counter       int16
	startTime     int64
	nextStartTime int64
	idPrefix      string
	lock          sync.Mutex
)

//MessageClientId = ip  + pid + classloaderid + counter + time
//4 bytes for ip ,
//2 bytes for pid,
//4 bytes for  classloaderid(for java,go put 0)

//2 bytes for counter,
//4 bytes for timediff, //(time.Now().UnixNano() - startTime) / 1000000) divide 1000000 because use time millis
func generatorMessageClientId() (uniqMessageId string) {
	defer lock.Unlock()
	lock.Lock()
	if len(idPrefix) == 0 {
		idPrefix = generatorMessageClientIdPrefix()
	}
	if time.Now().UnixNano() > nextStartTime {
		startTime, nextStartTime = getStartAndNextStartTime()
	}
	counter = counter + 1
	var buf2 = bytes.NewBuffer([]byte{})
	binary.Write(buf2, binary.BigEndian, int32((time.Now().UnixNano()-startTime)/1000000))
	binary.Write(buf2, binary.BigEndian, counter)
	uniqMessageId = idPrefix + bytes2string(buf2.Bytes())
	return
}

//GeneratorMessageOffsetId generator message offsetId
func GeneratorMessageOffsetId(storeHost []byte, port int32, commitOffset int64) (messageOffsetId string) {
	var buf = bytes.NewBuffer([]byte{})
	binary.Write(buf, binary.BigEndian, storeHost)
	binary.Write(buf, binary.BigEndian, port)
	binary.Write(buf, binary.BigEndian, commitOffset)
	idPrefix := buf.Bytes()
	messageOffsetId = bytes2string(idPrefix)
	return
}
func generatorMessageClientIdPrefix() (messageClientIdPrefix string) {
	var (
		idPrefix      []byte
		ip4Bytes      []byte
		pid           int16
		classloaderId int32 = -1 // golang don't have this
	)
	ip4Bytes = GetIp4Bytes()
	pid = int16(os.Getpid())
	var buf = bytes.NewBuffer([]byte{})
	binary.Write(buf, binary.BigEndian, ip4Bytes)
	binary.Write(buf, binary.BigEndian, pid)
	binary.Write(buf, binary.BigEndian, classloaderId)
	idPrefix = buf.Bytes()
	messageClientIdPrefix = bytes2string(idPrefix)
	return
}
func getStartAndNextStartTime() (thisMonthFirstDay12 int64, nextMonthFirstDay12 int64) {
	now := time.Now()
	year := now.Year()
	month := now.Month()
	thisMonthFirstDay12 = time.Date(year, month, 1, 0, 0, 0, 0, time.Local).UnixNano()
	month = month + 1
	if month > 12 {
		month = month - 12
		year = year + 1
	}
	nextMonthFirstDay12 = time.Date(year, month, 1, 0, 0, 0, 0, time.Local).UnixNano()
	return
}
func bytes2string(bytes []byte) (ret string) {
	for _, oneByte := range bytes {
		hexStr := strconv.FormatInt(int64(oneByte), 16)
		if len(hexStr) < 2 {
			hexStr = "0" + hexStr
		}
		ret = ret + hexStr
	}
	ret = strings.ToUpper(ret)
	return
}

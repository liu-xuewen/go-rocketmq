package header

import "strconv"

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
	queueID, err := strconv.ParseInt(headerMap["queueId"].(string), 10, 32)
	if err != nil {
		queueID = -1
	}
	s.QueueId = int32(queueID)
	queueOffset, err := strconv.ParseInt(headerMap["queueOffset"].(string), 10, 64)
	if err != nil {
		queueOffset = -1
	}
	s.QueueOffset = queueOffset
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

package header

//SearchOffsetRequestHeader of CustomerHeader
type SearchOffsetRequestHeader struct {
	Topic     string `json:"topic"`
	QueueId   int32  `json:"queueId"`
	Timestamp int64  `json:"timestamp"`
}

//FromMap convert map[string]interface to struct
func (s *SearchOffsetRequestHeader) FromMap(headerMap map[string]interface{}) {
	//s.Topic = headerMap["topic"].(string)
	//s.Topic = headerMap["queueId"].(string)
	//s.Timestamp = StrToInt64WithDefaultValue(headerMap["timestamp"].(string), -1)
	return
}

package header

//GetMaxOffsetRequestHeader of CustomerHeader
type GetMaxOffsetRequestHeader struct {
	Topic   string `json:"topic"`
	QueueId int32  `json:"queueId"`
}

//FromMap convert map[string]interface to struct
func (g *GetMaxOffsetRequestHeader) FromMap(headerMap map[string]interface{}) {
	return
}

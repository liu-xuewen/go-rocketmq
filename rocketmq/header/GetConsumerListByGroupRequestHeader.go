package header

type GetConsumerListByGroupRequestHeader struct {
	ConsumerGroup string `json:"consumerGroup"`
}

//FromMap convert map[string]interface to struct
func (g *GetConsumerListByGroupRequestHeader) FromMap(headerMap map[string]interface{}) {
	return
}

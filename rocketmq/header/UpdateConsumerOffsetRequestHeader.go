package header

type UpdateConsumerOffsetRequestHeader struct {
	ConsumerGroup string
	Topic         string
	QueueId       int32
	CommitOffset  int64
}

func (g *UpdateConsumerOffsetRequestHeader) FromMap(headerMap map[string]interface{}) {
	return
}

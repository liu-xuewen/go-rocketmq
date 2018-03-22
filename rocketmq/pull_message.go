package rocketmq

type PullRequest struct {
	consumerGroup string
	messageQueue  *MessageQueue
	nextOffset    int64
}

type PullMessageService struct {
	pullRequestQueue chan *PullRequest
	consumer         *DefaultConsumer
}

func NewPullMessageService() *PullMessageService {
	return &PullMessageService{
		pullRequestQueue: make(chan *PullRequest, 1024),
	}
}

func (self *PullMessageService) start() {
	for {
		pullRequest := <-self.pullRequestQueue
		self.consumer.pullMessage(pullRequest)
	}
}

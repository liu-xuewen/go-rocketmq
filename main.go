package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"time"

	"github.com/powxiao/go-rocketmq/rocketmq"
)

/*
1. 消费的时候没有获取到MsgId： decode message 需要设置msgID
2. 生产者数据已经发送，但是不是立即消费： 不能立即获取到consumerID
3. 怎么样才能获取ConsumerIDListByGroup成功:
*/
var (
	nameServerAddress = "127.0.0.1:9876" //address split by ;  (for example 192.168.1.1:9876;192.168.1.2:9876)
	testTopic         = "GoLangRMQ"
	testProducerGroup = "GoLangProducer"
	testConsumerGroup = "GoLangConsumer"
	conf              = &rocketmq.Config{
		NameServer:   nameServerAddress,
		InstanceName: "DEFAULT",
	}
)

type publishCallBack struct{}

func (p *publishCallBack) OnSuccess(result *rocketmq.SendResult) {
	log.Printf("%v", result.String())
}

func producerSync() {
	var producer = rocketmq.NewDefaultProducer(testProducerGroup, conf)
	producer.Start()
	var message = &rocketmq.MessageExt{}
	message.Topic = testTopic
	for i := 0; i < 10; i++ {
		message.Body = []byte(fmt.Sprintf("%d", i))
		_, err := producer.Send(message)
		if err != nil {
			log.Printf("fail to send message %v", err)
		} else {
			log.Printf("send %v", i)
		}
		time.Sleep(time.Second)
	}
}

func producerOneWay() {
	var producer = rocketmq.NewDefaultProducer(testProducerGroup, conf)
	producer.Start()
	var message = &rocketmq.MessageExt{}
	message.Topic = testTopic
	for i := 0; i < 10; i++ {
		message.Body = []byte(fmt.Sprintf("%d", i))
		producer.SendOneWay(message)
	}
}

func producerAsync() {
	var producer = rocketmq.NewDefaultProducer(testProducerGroup, conf)
	producer.Start()
	var message = &rocketmq.MessageExt{}
	message.Topic = testTopic
	for i := 0; i < 10; i++ {
		message.Body = []byte(fmt.Sprintf("%d", i))
		producer.SendAsync(message, &publishCallBack{})
	}
}

/*生产者和消费者相关测试*/
func main() {
	log.Printf("start main")

	consumer, err := rocketmq.NewDefaultConsumer(testConsumerGroup, conf)
	if err != nil {
		log.Println(err)
		return
	}
	consumer.Subscribe(testTopic, "*")
	consumer.RegisterMessageListener(func(msgs []*rocketmq.MessageExt) error {
		for _, msg := range msgs {
			log.Printf("[%s] msgBody[%s]", msg.MsgId, string(msg.Body))
		}
		return nil
	})
	consumer.Start()
	producerOneWay()
	s := make(chan os.Signal, 1)
	signal.Notify(s, syscall.SIGINT, syscall.SIGTERM)
	<-s
}

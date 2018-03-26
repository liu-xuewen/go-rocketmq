package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/powxiao/go-rocketmq/rocketmq"
)

var (
	nameServerAddress = "127.0.0.1:9876" //address split by ;  (for example 192.168.1.1:9876;192.168.1.2:9876)
	testTopic         = "GoLangRMQ"
	testProducerGroup = "GoLangProducer"
	testConsumerGroup = "GoLangConsumer"
)

/*生产者和消费者相关测试*/
func main() {

	conf := &rocketmq.Config{
		NameServer:   nameServerAddress,
		InstanceName: "DEFAULT",
	}

	consumer, err := rocketmq.NewDefaultConsumer(testConsumerGroup, conf)
	if err != nil {
		log.Println(err)
		return
	}
	consumer.Subscribe(testTopic, "*")
	consumer.RegisterMessageListener(func(msgs []*rocketmq.MessageExt) error {
		for _, msg := range msgs {
			log.Printf("[%s] msgBody[%s]", msg.MsgId, string(msg.Body))
			//spew.Dump(msg)
		}
		return nil
	})
	consumer.Start()

	var producer = rocketmq.NewDefaultProducer(testProducerGroup, conf)
	producer.Start()
	//start send test message
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
	}
	s := make(chan os.Signal, 1)
	signal.Notify(s, syscall.SIGINT, syscall.SIGTERM)
	<-s
}

package main

import (
	"log"

	"github.com/powxiao/go-rocketmq/client/rocketmq"
)

/*生产者和消费者相关测试*/
func main() {
	var (
		nameServerAddress = "127.0.0.1:9876" //address split by ;  (for example 192.168.1.1:9876;192.168.1.2:9876)
		testTopic         = "GoLangRMQ"
		testProducerGroup = "GoLangProducerGroup"
		testConsumerGroup = "GoLangConsumerGroup"
	)
	conf := &rocketmq.Config{
		NameServer:   nameServerAddress,
		InstanceName: "DEFAULT",
	}
	var producer = rocketmq.NewDefaultProducer(testProducerGroup, conf)
	producer.Start()

	consumer, err := rocketmq.NewDefaultConsumer(testConsumerGroup, conf)
	if err != nil {
		log.Println(err)
		return
	}
	consumer.Subscribe(testTopic, "*")
	consumer.RegisterMessageListener(func(msgs []*rocketmq.MessageExt) error {
		//successIndex := -1
		for _, msg := range msgs {
			log.Printf("receiveMessage messageId=[%s] messageBody=[%s]", msg.MsgId, string(msg.Body))
			//successIndex = index
		}
		return nil
		//return rocketmqm.ConsumeConcurrentlyResult{ConsumeConcurrentlyStatus: rocketmqm.CONSUME_SUCCESS, AckIndex: successIndex}
	})

	//start send test message
	for {
		var message = &rocketmq.MessageExt{}
		message.Topic = testTopic
		message.Body = []byte("hello World")
		result, err := producer.Send(message)
		log.Printf("test sendMessageResult result=[%+v] err=[%s]", result, err)
	}
}

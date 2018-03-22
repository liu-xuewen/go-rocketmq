package main

import (
	"log"

	"github.com/powxiao/go-rocketmq/client/rocketmq"
)

//rocketmq "github.com/StyleTang/incubator-rocketmq-externals/rocketmq-go/api"
//"github.com/StyleTang/incubator-rocketmq-externals/rocketmq-go/api/model"
//"github.com/didapinchenggit/go_rocket_mq"

//func didaConsumer() {
//	conf := &rocketmq.Config{
//		Nameserver:   "127.0.0.1:9876",
//		InstanceName: "DEFAULT",
//	}
//	consumer, err := rocketmq.NewDefaultConsumer("TestConsumerGroup", conf)
//	if err != nil {
//		panic(err)
//	}
//	consumer.Subscribe("GoLangRocketMQ", "*")
//	consumer.RegisterMessageListener(func(msgs []*rocketmq.MessageExt) error {
//		atomic.AddInt32(&totalNum, int32(len(msgs)))
//		for i, msg := range msgs {
//			log.Printf("%v,%v", i, string(msg.Body))
//		}
//		return nil
//	})
//	consumer.Start()
//}

//func defaultConsumer() {
//	var (
//		nameServerAddress = "127.0.0.1:9876" //address split by ;  (for example 192.168.1.1:9876;192.168.1.2:9876)
//		testTopic         = "GoLangRocketMQ"
//		testConsumerGroup = "TestConsumerGroup"
//	)
//	conf := &mq_go.Config{
//		NameServer:   nameServerAddress,
//		InstanceName: "DEFAULT",
//	}
//	consumer, err := mq_go.NewDefaultConsumer(testConsumerGroup, conf)
//	if err != nil {
//		panic(err)
//	}
//	consumer.Subscribe(testTopic, "*")
//	consumer.RegisterMessageListener(func(msgs []*mq_go.MessageExt) error {
//		atomic.AddInt32(&totalNum, int32(len(msgs)))
//		for _, msg := range msgs {
//			log.Printf("msg:%+v", string(msg.Body))
//		}
//
//		return nil
//	})
//	consumer.Start()
//}

//func StyleConsumer() {
//	var (
//		nameServerAddress = "127.0.0.1:9876" //address split by ;  (for example 192.168.1.1:9876;192.168.1.2:9876)
//		testTopic         = "GoLangRocketMQ"
//		testConsumerGroup = "TestConsumerGroup"
//	)
//	// init rocketMQClientInstance
//	rocketMQClientInstance := rocketmq.InitRocketMQClientInstance(nameServerAddress)
//	// 1.init rocketMQConsumer
//	// 2.subscribe topic and register our function to message listener
//	// 3.register it
//	var consumer = rocketmq.NewDefaultMQPushConsumer(testConsumerGroup)
//	consumer.Subscribe(testTopic, "*")
//	consumer.RegisterMessageListener(func(messageList []rocketmqm.MessageExt) rocketmqm.ConsumeConcurrentlyResult {
//		successIndex := -1
//		atomic.AddInt32(&totalNum, int32(len(messageList)))
//		for index, msg := range messageList {
//
//			log.Printf("test receiveMessage messageId=[%s] messageBody=[%s]", msg.MsgId(), string(msg.Body()))
//			// call your function
//			successIndex = index
//		}
//		return rocketmqm.ConsumeConcurrentlyResult{ConsumeConcurrentlyStatus: rocketmqm.CONSUME_SUCCESS, AckIndex: successIndex}
//	})
//	rocketMQClientInstance.RegisterConsumer(consumer)
//
//	// start rocketMQ client instance
//	rocketMQClientInstance.Start()
//}

//func StyleProduer() {
//	var (
//		nameServerAddress = "127.0.0.1:9876" //address split by ;  (for example 192.168.1.1:9876;192.168.1.2:9876)
//		testTopic         = "GoLangRocketMQ"
//		testProducerGroup = "TestProducerGroup"
//	)
//	// init rocketMQClientInstance
//	rocketMQClientInstance := rocketmq.InitRocketMQClientInstance(nameServerAddress)
//	// init rocketMQProducer and register it
//	var producer = rocketmq.NewDefaultMQProducer(testProducerGroup)
//	rocketMQClientInstance.RegisterProducer(producer)
//
//	// start rocketMQ client instance
//	rocketMQClientInstance.Start()
//
//	//start send test message
//	var message = rocketmqm.NewMessage()
//	message.SetTopic(testTopic)
//
//	for i := 0; i < 1000; i++ {
//		message.SetBody([]byte(fmt.Sprintf("%d %0x16", i, rand.Uint64())))
//		res, err := producer.Send(message)
//		if err != nil {
//			log.Printf("send error %+v", err)
//			return
//		}
//		log.Printf("[%d]send ok %+v", i, res)
//	}
//}

//func defaultProducer() {
//	var (
//		nameServerAddress = "127.0.0.1:9876" //address split by ;  (for example 192.168.1.1:9876;192.168.1.2:9876)
//		testTopic         = "GoLangRocketMQ"
//		testConsumerGroup = "TestConsumerGroup"
//	)
//	produer := rocketmq.NewDefaultProducer(testConsumerGroup, &rocketmq.Config{NameServer: nameServerAddress, InstanceName: "DEFAULT"})
//	produer.Start()
//	message := &rocketmq.MessageExt{}
//	message.Topic = testTopic
//	for i := 0; i < 1000; i++ {
//		message.Body = []byte(fmt.Sprintf("%d %0x16", i, rand.Uint64()))
//		res, err := produer.Send(message)
//		if err != nil {
//			log.Printf("send error %+v", err)
//			return
//		}
//		log.Printf("[%d]send ok %+v", i, res)
//	}
//}

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
			_ = msg
			//log.Printf("receiveMessage messageId=[%s] messageBody=[%s]", msg.MsgId, string(msg.Body))
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
		_, err := producer.Send(message)
		if err != nil {
			log.Printf("fail to send message %v", err)
		}
		//log.Printf("test sendMessageResult result=[%+v] err=[%s]", result, err)
	}
}

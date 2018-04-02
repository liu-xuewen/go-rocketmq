package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"

	"github.com/StyleTang/incubator-rocketmq-externals/rocketmq-go/api"
	"github.com/StyleTang/incubator-rocketmq-externals/rocketmq-go/api/model"
	didarocketmq "github.com/didapinchenggit/go_rocket_mq"
	powrocketmq "github.com/powxiao/go-rocketmq/rocketmq"
)

/*
1. 消费的时候没有获取到MsgId： decode message 需要设置msgID
2. 生产者数据已经发送，但是不是立即消费： 不能立即获取到consumerID
3. 怎么样才能获取ConsumerIDListByGroup成功:

subscribe
processQueue
registerProcessor

*/
var (
	nameServerAddress = "127.0.0.1:9876"
	testTopic         = "GoLangRMQ"
	//nameServerAddress = "10.1.51.48:9876" //address split by ;  (for example 192.168.1.1:9876;192.168.1.2:9876)
	//testTopic         = "wsd_vrp_slide_pool"
	testTopics        = []string{"GoLangRMQ1", "GoLangRMQ2", "GoLangRMQ3"}
	testProducerGroup = "GoLangProducer"
	testConsumerGroup = "wsd_vrp_source_slide" //"GoLangConsumer"
	conf              = &powrocketmq.Config{
		NameServer:   nameServerAddress,
		InstanceName: "DEFAULT",
	}
)

type publishCallBack struct{}

//func (p *publishCallBack) OnSuccess(result *rocketmq.SendResult) {
//	//log.Printf("%v", result.String())
//}
//
////10.002s - 197.463mb/s - 2070549.631ops/s - 4.830us/op
//func producerSync(topic string) {
//	runFor := 2 * time.Second //10 * time.Second
//	var producer = rocketmq.NewDefaultProducer(testProducerGroup, conf)
//	producer.Start()
//	var message = &rocketmq.MessageExt{}
//	message.Topic = topic
//	message.Body = make([]byte, 100)
//	var msgCount int64
//	endTime := time.Now().Add(runFor)
//	for {
//		_, err := producer.Send(message)
//		if err != nil {
//			log.Printf("fail to send message %v", err)
//			return
//		}
//		msgCount++
//		if time.Now().After(endTime) {
//			break
//		}
//	}
//	atomic.AddInt64(&totalMsgCount, msgCount)
//}
//
func producerSyncCount(topic string) {
	var producer = powrocketmq.NewDefaultProducer(testProducerGroup, conf)
	producer.Start()
	var message = &powrocketmq.MessageExt{}
	message.Topic = topic
	var msgCount int64
	for i := 0; i < 60; i++ {
		message.Body = []byte(fmt.Sprintf("%d", i))
		_, err := producer.Send(message)
		if err != nil {
			log.Printf("fail to send message %v", err)
			return
		} else {
			log.Printf("send %v", i)
		}
		msgCount++
	}
	atomic.AddInt64(&totalMsgCount, msgCount)
}

//
//func producerOneWay() {
//	var producer = rocketmq.NewDefaultProducer(testProducerGroup, conf)
//	producer.Start()
//	var message = &rocketmq.MessageExt{}
//	message.Topic = testTopic
//	for i := 0; i < 10; i++ {
//		message.Body = []byte(fmt.Sprintf("%d", i))
//		producer.SendOneWay(message)
//	}
//}
//
//func producerAsync() {
//	var producer = rocketmq.NewDefaultProducer(testProducerGroup, conf)
//	producer.Start()
//	var message = &rocketmq.MessageExt{}
//	message.Topic = testTopic
//	for i := 0; i < 10; i++ {
//		message.Body = []byte(fmt.Sprintf("%d", i))
//		producer.SendAsync(message, &publishCallBack{})
//	}
//}

func producerOrdered() {
	//var producer = rocketmq.NewDefaultProducer(testProducerGroup, conf)
	//producer.Start()
	//var message = &rocketmq.MessageExt{}
	//message.Topic = testTopic
	//for i := 0; i < 20; i++ {
	//	orderID := i % 5
	//	str := fmt.Sprintf("KEY[%d]-ORDERID[%d]", i, orderID)
	//	message.Body = []byte(str)
	//	_, err := producer.SendOrderly(message, orderID)
	//	if err != nil {
	//		log.Printf("fail to send message %v", err)
	//	} else {
	//		log.Printf("send %v", str)
	//	}
	//}
}

func consumeRunStyle(topic string) {
	rocketMQClientInstance := rocketmq.InitRocketMQClientInstance(nameServerAddress)
	// 1.init rocketMQConsumer
	// 2.subscribe topic and register our function to message listener
	// 3.register it
	var consumer = rocketmq.NewDefaultMQPushConsumer(testConsumerGroup)
	consumer.Subscribe(topic, "*")
	consumer.RegisterMessageListener(func(messageList []rocketmqm.MessageExt) rocketmqm.ConsumeConcurrentlyResult {
		successIndex := -1
		for index, msg := range messageList {
			log.Printf("messageId=[%s] messageBody=[%s]", msg.MsgId(), string(msg.Body()))
			// call your function
			successIndex = index
			atomic.AddInt64(&totalMsgConsume, 1)
		}
		return rocketmqm.ConsumeConcurrentlyResult{ConsumeConcurrentlyStatus: rocketmqm.CONSUME_SUCCESS, AckIndex: successIndex}
	})
	rocketMQClientInstance.RegisterConsumer(consumer)

	// start rocketMQ client instance
	rocketMQClientInstance.Start()
}

func consumerRun(topic string) {
	consumer, err := powrocketmq.NewDefaultConsumer(testConsumerGroup, conf)
	if err != nil {
		log.Println(err)
		return
	}
	consumer.Subscribe(topic, "*")
	consumer.RegisterMessageListener(func(msgs []*powrocketmq.MessageExt) error {
		for _, msg := range msgs {
			//_ = msg
			log.Printf("msgBody[%s]", string(msg.Body))
			atomic.AddInt64(&totalMsgConsume, 1)
		}
		return nil
	})
	consumer.Start()
}

func consumeRunDida(topic string) {
	conf := &didarocketmq.Config{
		Nameserver:   nameServerAddress,
		InstanceName: "DEFAULT",
	}
	consumer, err := didarocketmq.NewDefaultConsumer(testConsumerGroup, conf)
	if err != nil {
		panic(err)
	}
	consumer.Subscribe(topic, "*")
	consumer.RegisterMessageListener(func(msgs []*didarocketmq.MessageExt) error {
		for i, msg := range msgs {
			log.Println(i, string(msg.Body))
			atomic.AddInt64(&totalMsgConsume, 1)
		}
		return nil
	})
	consumer.Start()
}

var totalMsgCount int64
var totalMsgConsume int64

//func performanceRun() {
//	size := 100
//	start := time.Now()
//	msg := make([]byte, size)
//	log.Printf("%+v", msg)
//	var wg sync.WaitGroup
//	wg.Add(3)
//	for i := 0; i < len(testTopics); i++ {
//		go func(topic string) {
//			//producerSync(topic)
//			consumerRun(topic)
//			wg.Done()
//		}(testTopics[i])
//	}
//	wg.Wait()
//	duration := time.Now().Sub(start)
//	tmc := atomic.LoadInt64(&totalMsgCount)
//	log.Printf("duration: %s - %.03fmb/s - %.03fops/s - %.03fus/op",
//		duration,
//		float64(tmc*int64(size))/duration.Seconds()/1024/1024,
//		float64(tmc)/duration.Seconds(),
//		float64(duration/time.Microsecond)/float64(tmc))
//}

/*
重新consume会出现从起始位置开始消费
main.go:230: totalMsgCount 60 totalMsgConsume 66795
*/

func main() {
	producerSyncCount(testTopic)
	consumerRun(testTopic)
	//consumeRunDida(testTopic)
	//consumeRunStyle(testTopic)
	s := make(chan os.Signal, 1)
	signal.Notify(s, syscall.SIGINT, syscall.SIGTERM)
	<-s
	tmc := atomic.LoadInt64(&totalMsgCount)
	tmcc := atomic.LoadInt64(&totalMsgConsume)
	log.Printf("totalMsgCount %v totalMsgConsume %v", tmc, tmcc)
}

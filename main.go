package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"

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

// push 方式
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

func consumerPullRun(topic string) {
	consumer, err := powrocketmq.NewDefaultConsumer(testConsumerGroup, conf)
	if err != nil {
		log.Println(err)
		return
	}
	result := consumer.Pull(topic, "*", 100)
	log.Printf("%v", result)
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

# go-rocketmq
## nameserver
* broker discovery
* router info management (topic)
*

## broker
* one broker = master (0) + slaves
* message queues （each topic has several mq）


## NSQ vs RocketMQ

|功能项  |NSQ|RocketMQ|
|-------|---|--------|
|服务发现|Nsqlookupd|Nameserver|
|传输层安全性|TLS|unknown|
|消息是否可持久化|不支持|支持|


NSQ特性
* 支持无 SPOF 的分布式拓扑
* 水平扩展(没有中间件，无缝地添加更多的节点到集群)
* 低延迟消息传递 (性能)
* 结合负载均衡和多播消息路由风格
* 擅长面向流媒体(高通量)和工作(低吞吐量)工作负载
* 主要是内存中(除了高水位线消息透明地保存在磁盘上)
* 运行时发现消费者找到生产者服务(nsqlookupd)
* 传输层安全性 (TLS)
* 数据格式不可知
* 一些依赖项(容易部署)和健全的，有界，默认配置
* 任何语言都有简单 TCP 协议支持客户端库
* HTTP 接口统计、管理行为和生产者(不需要客户端库发布)
* 为实时检测集成了 statsd
* 健壮的集群管理界面 (nsqadmin)

RocketMQ特性
* Protocol and Specification: Pull model support TCP, JMS, OpenMessaging	
* Ordered Message:Ensure strict ordering of messages,and can scale out gracefully	
* Scheduled Message: Supported
* Batched Message:Supported, with sync mode to avoid message loss
* BroadCast Message:Supported
* Message Filter:Supported, property filter expressions based on SQL92
* Server Triggered Redelivery:Supported
* Message Storage:High performance and low latency file storage
* Message Retroactive:Supported timestamp and offset two indicates
* Message Priority:Not Supported
* High Availability and Failover:Supported, Master-Slave model, without another kit
* Message Track:Supported
* Configuration:Work out of box,user only need to pay attention to a few configurations
* Management and Operation Tools:Supported, rich web and terminal command to expose core metrics
* 支持发布/订阅（Pub/Sub）和点对点（P2P）消息模型
* 在一个队列中可靠的先进先出（FIFO）和严格的顺序传递
* 支持拉（pull）和推（push）两种消息模式
* 单一队列百万消息的堆积能力
* 支持多种消息协议，如 JMS、MQTT 等
* 分布式高可用的部署架构,满足至少一次消息传递语义
* 提供 docker 镜像用于隔离测试和云集群部署
* 提供配置、指标和监控等功能丰富的 Dashboard


FAQ
* 如何保证消息消费成功
* 启动时候消费的起始位置
* 是否保证消息顺序
* 是否支持集群消费广播消费
* 是否支持pull/push 消息模式
* 消息持久化
虽然系统支持消息持久化存储在磁盘中（通过 --mem-queue-size ），不过默认情况下消息都在内存中.
如果将 --mem-queue-size 设置为 0，所有的消息将会存储到磁盘。我们不用担心消息会丢失，nsq 内部机制保证在程序关闭时将队列中的数据持久化到硬盘，重启后就会恢复
NSQ 没有内置的复制机制，却有各种各样的方法管理这种权衡，比如部署拓扑结构和技术，在容错的时候从属并持久化内容到磁盘。
* 消息最少会被投递一次
因为各种原因，消息可以被投递多次（客户端超时，连接失效，重新排队，等等）。由客户端负责操作。
* 接收到的消息是无序的
不要依赖于投递给消费者的消息的顺序。
和投递消息机制类似，它是由重新队列(requeues)，内存和磁盘存储的混合导致的，实际上，节点间不会共享任何信息。
它是相对的简单完成疏松队列，（例如，对于某个消费者来说，消息是有次序的，但是不能给你作为一个整体跨集群），通过使用时间窗来接收消息，并在处理前排序（虽然为了维持这个变量，必须抛弃时间窗外的消息）。
* 消费者最终找出所有话题的生产者
这个服务(nsqlookupd) 被设计成最终一致性。nsqlookupd 节点不会维持状态，也不会回答查询。
网络分区并不会影响可用性，分区的双方仍然能回答查询。部署性拓扑可以显著的减轻这类问题。


* Unfortunately, Kafka can not meet our requirements especially in terms of low latency and high reliability
package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"log"
)

/**
*
* @author yth
* @language go
* @since 2023/6/8 21:51
 */

func main() {
	config := sarama.NewConfig()
	// 用于指示生产者在成功发送消息后是否返回成功的响应。
	config.Producer.Return.Successes = true
	// Kafka集群的地址
	brokers := []string{"192.168.10.102:9092"}
	client, err := sarama.NewClient(brokers, config)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer func() { _ = client.Close() }()

	// 创建一个Kafka生产者并发送消息
	producer, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer func() { _ = producer.Close() }()

	// 创建主题
	admin, err := sarama.NewClusterAdmin([]string{"192.168.10.102:9092"}, config)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = admin.Close() }()
	topic := "second"
	detail := &sarama.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}
	err = admin.CreateTopic(topic, detail, false)

	// 创建消息
	topic = "second"
	message := "Hello, Kafka"
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}

	// 发送消息
	_, _, err = producer.SendMessage(msg)
	if err != nil {
		fmt.Println(err)
		return
	}

	// 创建一个Kafka消费者并发送消息
	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer func() { _ = consumer.Close() }()

	// 指定消息的 消费主题 和 offset
	topic = "second"
	partition := int32(0)
	offset := sarama.OffsetOldest
	partitionConsumer, err := consumer.ConsumePartition(topic, partition, offset)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer func() { _ = partitionConsumer.Close() }()
	// 遍历消费到的消息
	for msg := range partitionConsumer.Messages() {
		fmt.Println("Received message: %s", string(msg.Value))
	}

}

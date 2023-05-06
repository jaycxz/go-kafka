package kafka

import (
	"context"
	"encoding/json"
	"log"
	"math/rand"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

type FinanceData struct {
	Name string
	Value string
}

func Produce(topic string) {
	// partition := 0

	// conn, err := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", topic, partition)
	// if err != nil {
	// 	log.Fatal("failed to dial leader:", err)
	// }

	// conn.SetWriteDeadline(time.Now().Add(10 * time.Second))


	var idList []string = []string{"goog", "aapl", "msft"}
	var kafkaMessages []kafka.Message

	for _, id := range idList {
		random_val := rand.Intn(1000)
		val := strconv.Itoa(random_val)
		data, _ := json.Marshal(FinanceData{
			Name: id,
			Value: val,
		})

		kafkaMessages = append(kafkaMessages, kafka.Message{
			Key: []byte(id),
			Value: data,
		})
	}

	w := &kafka.Writer{
		Addr:     kafka.TCP("localhost:9092", "localhost:9093", "localhost:9094"),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}
	
	err := w.WriteMessages(context.Background(),
		kafkaMessages...,
	)

	if err != nil {
		log.Fatal("failed to write messages:", err)
	}
	
	if err := w.Close(); err != nil {
		log.Fatal("failed to close writer:", err)
	}
}

func MockFinancialData() {
	topic := "financeData"
	
	for {
		Produce(topic)
		time.Sleep(1000 * time.Millisecond)
	}
}
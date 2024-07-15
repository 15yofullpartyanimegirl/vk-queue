package main

import (
	"context"
	"encoding/binary"
	"log"
	"os"

	kafka "github.com/segmentio/kafka-go"
)

type Headers []kafka.Header

func testHeaders() Headers {
	pubDateBlob := make([]byte, 8)
	fetchTimeBlob := make([]byte, 8)
	firstFetchTimeBlob := make([]byte, 8)

	binary.BigEndian.PutUint64(pubDateBlob, uint64(10))
	binary.BigEndian.PutUint64(fetchTimeBlob, uint64(10))
	binary.BigEndian.PutUint64(firstFetchTimeBlob, uint64(10))

	r := Headers{
		kafka.Header{
			Key:   "url",
			Value: []byte("test"),
		},
		kafka.Header{
			Key:   "pubDate",
			Value: pubDateBlob,
		},
		kafka.Header{
			Key:   "fetchTime",
			Value: fetchTimeBlob,
		},
		kafka.Header{
			Key:   "text",
			Value: []byte("tmp"),
		},
		kafka.Header{
			Key:   "firstFetchTime",
			Value: firstFetchTimeBlob,
		},
	}
	return r
}

func main() {
	// get kafka writer using environment variables.
	kafkaURL := os.Getenv("kafkaURL")
	topic := os.Getenv("topic")

	log.Println("start producing ... !!")
	// make a writer that produces to topic-A, using the least-bytes distribution
	w := &kafka.Writer{
		Addr:     kafka.TCP(kafkaURL),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}

	err := w.WriteMessages(context.Background(),
		kafka.Message{
			Key:     []byte("Key-D0"),
			Value:   []byte(nil),
			Headers: testHeaders(),
		},
	)
	if err != nil {
		log.Fatal("failed to write messages:", err)
	}

	if err := w.Close(); err != nil {
		log.Fatal("failed to close writer:", err)
	}
}

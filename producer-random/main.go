package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"log"
	"os"

	kafka "github.com/segmentio/kafka-go"
)

type Document struct {
	Url            string `json:"url"`
	PubDate        uint64 `json:"pubdate"`
	FetchTime      uint64 `json:"fetchtime"`
	Text           string `json:"text"`
	FirstFetchTime uint64 `json:"firstfetchtime"`
}

type Headers []kafka.Header

func formHeaders(doc *Document) Headers {
	// convert Document -> Headers struct
	pubDateBlob := make([]byte, 8)
	fetchTimeBlob := make([]byte, 8)
	firstFetchTimeBlob := make([]byte, 8)

	binary.BigEndian.PutUint64(pubDateBlob, doc.PubDate)
	binary.BigEndian.PutUint64(fetchTimeBlob, doc.FetchTime)
	binary.BigEndian.PutUint64(firstFetchTimeBlob, doc.FirstFetchTime)

	r := Headers{
		kafka.Header{
			Key:   "url",
			Value: []byte(doc.Url),
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
			Value: []byte(doc.Text),
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

	w := &kafka.Writer{
		Addr:                   kafka.TCP(kafkaURL),
		Topic:                  topic,
		Balancer:               &kafka.LeastBytes{},
		AllowAutoTopicCreation: true,
	}

	defer w.Close()

	// get example messages
	filepath := os.Getenv("msgfile")
	file, err := os.ReadFile(filepath)
	if err != nil {
		log.Fatal(err)
	}

	var docs []Document

	if err := json.Unmarshal(file, &docs); err != nil {
		log.Fatal(err)
	}

	log.Println(docs)

	//

	log.Println("start producing ... !!")
	// make a writer that produces to topic-A, using the least-bytes distribution

	for _, doc := range docs {
		log.Printf("%s document was added", doc.Url)
		err := w.WriteMessages(context.Background(),
			kafka.Message{
				Key:     []byte(doc.Url),
				Value:   []byte(nil),
				Headers: formHeaders(&doc),
			},
		)
		if err != nil {
			log.Fatal("failed to write messages:", err)
		}
	}

}

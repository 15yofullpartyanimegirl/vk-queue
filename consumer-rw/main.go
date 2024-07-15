package main

import (
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"log"
	"os"

	_ "github.com/lib/pq"
	kafka "github.com/segmentio/kafka-go"
)

func getKafkaReader(kafkaURL, topic, groupID string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{kafkaURL},
		GroupID:  groupID,
		Topic:    topic,
		MinBytes: 1,    // ?
		MaxBytes: 10e6, // 10MB
	})
}

func getDBInfo() string {
	host := os.Getenv("postgresHost")
	port := os.Getenv("postgresPort")
	user := os.Getenv("postgresUser")
	password := os.Getenv("postgresPassword")
	dbname := os.Getenv("postgresDBName")
	psqlInfo := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)
	return psqlInfo
}

type Document struct {
	url            string
	pubDate        uint64
	fetchTime      uint64
	text           string
	firstFetchTime uint64
}

func (d *Document) read(msg kafka.Message) {
	d.url = string(msg.Headers[0].Value)
	d.pubDate = binary.BigEndian.Uint64(msg.Headers[1].Value)
	d.fetchTime = binary.BigEndian.Uint64(msg.Headers[2].Value)
	d.text = string(msg.Headers[3].Value)
	d.firstFetchTime = binary.BigEndian.Uint64(msg.Headers[4].Value)
}

func (d *Document) readRow(rows *sql.Rows) {
	rows.Scan(&d.url, &d.pubDate, &d.fetchTime, &d.text, &d.firstFetchTime)
}

func docCheck(db *sql.DB, doc *Document) bool {
	var n int
	rows, err := db.Query("SELECT count(*) FROM public.documents WHERE url=$1", doc.url)
	if err != nil {
		log.Fatal("failed sql query: ", err)
	}
	defer rows.Close()
	rows.Scan(&n)
	return n > 0
}

func createDocument(db *sql.DB, doc *Document) {
	_, err := db.Exec("INSERT INTO public.documents VALUES ($1, $2, $3, $4, $5)", doc.url, doc.pubDate, doc.fetchTime, doc.text, doc.fetchTime)
	if err != nil {
		log.Fatal("failed sql query: ", err)
	}
	log.Println("new doc, insert operation has done")
}

func getDocument(db *sql.DB, doc *Document) Document {
	rows, err := db.Query("SELECT * FROM public.documents WHERE url=$1", doc.url)
	if err != nil {
		log.Fatal("failed sql query: ", err)
	}
	var r Document
	r.readRow(rows)
	return r
}

func updateDocument(db *sql.DB, doc *Document) {
	rows, err := db.Query("SELECT * FROM public.documents WHERE url=$1", doc.url)
	if err != nil {
		log.Fatal("failed sql query: ", err)
	}
	var image Document
	image.readRow(rows)
	if doc.fetchTime < image.firstFetchTime {
		_, err1 := db.Exec("UPDATE public.documents SET firstfetchtime=$1, pubdate=$2 WHERE url=$3", doc.fetchTime, doc.pubDate, doc.url)
		if err1 != nil {
			log.Fatal("failed to write messages:", err)
		}
	}
	// elseif doc.ft == image.ft message dublicte error ??
	if doc.fetchTime > image.fetchTime {
		_, err1 := db.Exec("UPDATE public.documents SET fetchtime=$1, textval=$2 WHERE url=$3", doc.fetchTime, doc.text, doc.url)
		if err1 != nil {
			log.Fatal("failed to write messages:", err)
		}
	}
	defer rows.Close()
}

type Headers []kafka.Header

func formHeaders(doc *Document) Headers {
	pubDateBlob := make([]byte, 8)
	fetchTimeBlob := make([]byte, 8)
	firstFetchTimeBlob := make([]byte, 8)

	binary.BigEndian.PutUint64(pubDateBlob, doc.pubDate)
	binary.BigEndian.PutUint64(fetchTimeBlob, doc.fetchTime)
	binary.BigEndian.PutUint64(firstFetchTimeBlob, doc.firstFetchTime)

	r := Headers{
		kafka.Header{
			Key:   "url",
			Value: []byte(doc.url),
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
			Value: []byte(doc.text),
		},
		kafka.Header{
			Key:   "firstFetchTime",
			Value: firstFetchTimeBlob,
		},
	}
	return r
}

func main() {
	// get kafka reader using environment variables.
	kafkaURL := os.Getenv("kafkaURL")
	topic := os.Getenv("topic")
	groupID := os.Getenv("groupID")
	reader := getKafkaReader(kafkaURL, topic, groupID)
	defer reader.Close()

	// get postgres connection
	psqlInfo := getDBInfo()
	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		log.Fatal(err)
	}
	if err = db.Ping(); err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// consuming
	log.Println("start consuming ... !!")

	msg, err := reader.ReadMessage(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	log.Println(msg.Headers)

	var doc Document
	doc.read(msg)

	// check doc id
	if docCheck(db, &doc) {
		createDocument(db, &doc)
	} else {
		updateDocument(db, &doc)
	}

	w := &kafka.Writer{
		Addr:     kafka.TCP(kafkaURL),
		Topic:    "out-queue-0",
		Balancer: &kafka.LeastBytes{},
	}

	image := getDocument(db, &doc)

	if err := w.WriteMessages(context.Background(),
		kafka.Message{
			Key:     []byte("Key-X0"),
			Value:   []byte(nil),
			Headers: formHeaders(&image),
		},
	); err != nil {
		log.Fatal("failed to write messages:", err)
	}

	if err := w.Close(); err != nil {
		log.Fatal("failed to close writer:", err)
	}

}

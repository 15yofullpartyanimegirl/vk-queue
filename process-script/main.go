package main

import (
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"

	_ "github.com/lib/pq"
	kafka "github.com/segmentio/kafka-go"
)

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
	Url            string
	PubDate        uint64
	FetchTime      uint64
	Text           string
	FirstFetchTime uint64
}

func (d *Document) read(msg kafka.Message) {
	d.Url = string(msg.Headers[0].Value)
	d.PubDate = binary.BigEndian.Uint64(msg.Headers[1].Value)
	d.FetchTime = binary.BigEndian.Uint64(msg.Headers[2].Value)
	d.Text = string(msg.Headers[3].Value)
	d.FirstFetchTime = binary.BigEndian.Uint64(msg.Headers[4].Value)
}

func (d *Document) readRow(rows *sql.Rows) {
	rows.Scan(&d.Url, &d.PubDate, &d.FetchTime, &d.Text, &d.FirstFetchTime)
}

func getDocument(db *sql.DB, doc *Document) Document {
	rows, err := db.Query("SELECT * FROM public.documents WHERE url=$1", doc.Url)
	if err != nil {
		log.Fatal("failed sql query: ", err)
	}
	var r Document
	r.readRow(rows)
	return r
}

func docCheck(db *sql.DB, doc *Document) (bool, error) {
	var n int
	rows, err := db.Query("SELECT count(*) FROM public.documents WHERE url=$1", doc.Url)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	rows.Scan(&n)
	return n > 0, nil
}

func createDocument(db *sql.DB, doc *Document) error {
	_, err := db.Exec("INSERT INTO public.documents VALUES ($1, $2, $3, $4, $5)", doc.Url, doc.PubDate, doc.FetchTime, doc.Text, doc.FetchTime)
	if err != nil {
		log.Println("createDocument func: failed sql query")
		return err
	}
	log.Println("new doc, insert operation has done")
	return nil
}

func updateDocument(db *sql.DB, doc *Document) error {
	image := getDocument(db, doc)

	if doc.FetchTime == image.FetchTime {
		log.Println("updateDocument func: duplicate case")
		return fmt.Errorf("dublicated docs")
	}
	if doc.FetchTime < image.FirstFetchTime {
		_, err := db.Exec("UPDATE public.documents SET firstfetchtime=$1, pubdate=$2 WHERE url=$3", doc.FetchTime, doc.PubDate, doc.Url)
		if err != nil {
			log.Println("updateDocument func: failed sql query")
			return err
		}
	}
	if doc.FetchTime > image.FetchTime {
		_, err := db.Exec("UPDATE public.documents SET fetchtime=$1, textval=$2 WHERE url=$3", doc.FetchTime, doc.Text, doc.Url)
		if err != nil {
			log.Println("updateDocument func: failed sql query")
			return err
		}
	}
	return nil
}

type Headers []kafka.Header

func formHeaders(doc *Document) Headers {
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

func process(doc *Document) (*Document, error) {
	// connect to DB
	// check: is doc in DB
	// create or update doc into DB
	// get cached doc from DB
	psqlInfo := getDBInfo()
	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		log.Println("process func: open db problem")
		return nil, err
	}
	if err := db.Ping(); err != nil {
		log.Println("process func: ping db problem")
		return nil, err
	}
	defer db.Close()

	docExistence, err := docCheck(db, doc)
	if err != nil {
		return doc, err
	}

	if docExistence {
		err := createDocument(db, doc)
		if err != nil {
			return doc, err
		}
	} else {
		err := updateDocument(db, doc)
		if err != nil {
			return nil, err
		} else if err == fmt.Errorf("dublicated docs") {
			return doc, err
		}
	}

	image := getDocument(db, doc)

	return &image, nil
}

type Processor interface {
	process(doc *Document) (*Document, error)
}

func main() {
	// CONNECTIONS DECLARATION
	kafkaURL := os.Getenv("kafkaURL")
	topic := os.Getenv("topic")
	groupID := os.Getenv("groupID")
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{kafkaURL},
		GroupID:  groupID,
		Topic:    topic,
		MaxBytes: 10e2,
	})
	defer reader.Close()

	writer := kafka.Writer{
		Addr:                   kafka.TCP(kafkaURL),
		Topic:                  "out-queue-0",
		Balancer:               &kafka.LeastBytes{},
		AllowAutoTopicCreation: true,
	}
	defer writer.Close()

	psqlInfo := getDBInfo()
	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		log.Fatal(err)
	}
	if err = db.Ping(); err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	//  DECLARATION END

	// LOOP: READ, process(), WRITE
	for {
		// consuming
		msg, err := reader.ReadMessage(context.Background())
		if err == io.EOF {
			break
		} else if err != nil {
			log.Fatal(err)
		}
		log.Println(msg.Headers)

		// doc initialization
		var doc Document
		doc.read(msg)

		// -
		image, err := process(&doc)
		if err == fmt.Errorf("dublicated docs") {
			// errors.Is() instead needed
			log.Println(err)
			err = nil
		} else if err != nil {
			log.Fatal(err)
		}

		// produce processed msg
		if err := writer.WriteMessages(context.Background(),
			kafka.Message{
				Key:     []byte(image.Url),
				Value:   []byte(nil),
				Headers: formHeaders(image),
			},
		); err != nil {
			log.Fatal("failed to write messages:", err)
		}
	}

}

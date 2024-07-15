package main

import (
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"reflect"

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

	// tableName := os.Getenv("tableName")
	urlValue := string(msg.Headers[0].Value)
	// pubDateValue := binary.BigEndian.Uint64(msg.Headers[1].Value)
	pubDateValue := binary.BigEndian.Uint64(msg.Headers[1].Value)
	fetchTimeValue := binary.BigEndian.Uint64(msg.Headers[2].Value)
	textValue := string(msg.Headers[3].Value)
	firstFetchTimeValue := binary.BigEndian.Uint64(msg.Headers[4].Value)
	log.Println(reflect.TypeOf(pubDateValue))
	// _, err = db.Exec("INSERT INTO $1 VALUES ($2, $3, $4, $5, $6)", tableName, urlValue, pubDateValue, fetchTimeValue, textValue, firstFetchTimeValue)
	_, err = db.Exec("INSERT INTO public.documents VALUES ($1, $2, $3, $4, $5)", urlValue+"2", pubDateValue, fetchTimeValue, textValue, firstFetchTimeValue)

	if err != nil {
		log.Fatal(err)
	}
}

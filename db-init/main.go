package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"

	_ "github.com/lib/pq"
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

func main() {
	psqlInfo := getDBInfo()

	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		log.Fatal(err)
	}
	if err = db.Ping(); err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE IF NOT EXISTS $1 (url TEXT NOT NULL PRIMARY KEY, pubDate INT, fetchTime INT,  textVal TEXT, firstFetchTime INT)", os.Getenv("tableName"))
	if err != nil {
		log.Fatal(err)
	}
}

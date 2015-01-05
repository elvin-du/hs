package main

import (
	"hs/database"
	"log"
	"time"
)

func main() {
	database.Start()
	go Query()
	go Query()
	go Query()
	go Query()

	time.Sleep(time.Second * 50)
	go Query()
	go Query()
	go Query()
	go Query()

	ch := make(chan byte)
	<-ch
}

func Query() {
	db, err := database.GetMMySQL()
	if nil != err {
		log.Fatal(err)
	}

	log.Println(db.Db().Raw.Ping())
	rows, _, err := db.Db().Query("select * from users")
	if nil != err {
		log.Println(err)
		return
	}

	for _, row := range rows {
		//for _, v := range row {

		//}
		log.Println(row.Int(0))
		log.Println(row.Str(1))
		log.Println(row.Str(2))
	}
}

package main

import (
	"hs/database"
	"log"
	"math/rand"
	"strconv"
	"time"
)

func main() {
	database.Start()
	//go Query()
	//go Query()
	//go Query()
	//go Query()
	go RedisGet()
	go RedisGet()
	go RedisGet()
	go RedisGet()
	go RedisGet()

	time.Sleep(time.Second * 50)
	go RedisGet()
	go RedisGet()
	go RedisGet()
	go RedisGet()
	go RedisGet()
	//go Query()
	//go Query()
	//go Query()
	//go Query()

	ch := make(chan byte)
	<-ch
}

func RedisGet() {
	redis, err := database.GetMRedis()
	if nil != err {
		log.Println(err)
		return
	}

	client := redis.Client()

	val := strconv.Itoa(rand.Int())
	err = client.Set("zoe", val).Err()
	if nil != err {
		log.Println(err)
		return
	}
	log.Println("set zoe value:", val)

	value, err := client.Get("zoe").Result()
	if nil != err {
		log.Println(err)
		return
	}
	log.Println(value)
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

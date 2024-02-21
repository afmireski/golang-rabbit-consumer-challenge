package main

import (
	// "bufio"
	"database/sql"
	"encoding/json"
	"strconv"

	"fmt"
	"log"
	// "os"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

func consumerClose(channelClose stream.ChannelClose) {
	event := <-channelClose
	fmt.Printf("Consumer: %s closed on the stream: %s, reason: %s \n", event.Name, event.StreamName, event.Reason)
}

type MessageInput struct {
	Email string `json:"email"`
	Name  string `json:"name"`
}

func setupConsumer(env *stream.Environment, chDB chan MessageInput, chConsumer chan *stream.Consumer) {
	consumeMessages := func(consumerContext stream.ConsumerContext, message *amqp.Message) {
		for _, raw := range message.Data {
			var data MessageInput
			json.Unmarshal(raw, &data)

			chDB <- data // Publica pro banco.
		}
	}

	requestStream := "secmob.request"

	consumer, err := env.NewConsumer(requestStream, consumeMessages,
		stream.NewConsumerOptions().
			SetConsumerName("secmob_request_consumer").
			SetCRCCheck(false).                             // Enable/Disable the CRC control.
			SetOffset(stream.OffsetSpecification{}.Next())) // start consuming from the beginning

	if err != nil {
		log.Fatalf("Falha ao criar consumidor: ", err)
	}

	chConsumer <- consumer
}

func connectDatabase() *sql.DB {
	db, err := sql.Open("mysql", "root:123456@/golang-rabbit")

	if err != nil {
		log.Fatalf("Falha ao conectar no banco", err)
	}

	db.SetConnMaxLifetime(time.Minute * 3)
	db.SetMaxOpenConns(0)
	db.SetMaxIdleConns(10)

	return db
}

func saveDataDb(db *sql.DB, chDB <- chan MessageInput, chID chan int64) {
	for {
		select {
			case data := <-chDB:
				stmtIns, err := db.Prepare("INSERT INTO user(email, name) VALUES(?, ?)")

				if err != nil {
					log.Fatalf("Falha ao preparar o Insert: ", err)
				}

				res, err := stmtIns.Exec(data.Email, data.Name)

				if err != nil {
					log.Fatalf("Falha ao executar o Insert: ", err)
				}

				stmtIns.Close()

				id, err := res.LastInsertId()
				
				chID <-  id // Publica pra outra fila.
		}
	}
}

func publish(producer *stream.Producer, ch <-chan int64) {
	for {
		select {
			case id := <-ch:
				message := amqp.NewMessage([]byte(strconv.FormatInt(id, 10)))

				err := producer.Send(message)

				if err != nil {
					log.Fatalf("Falha ao publicar no producer: ", err)
				}
		}
	}
}

func main() {
	// reader := bufio.NewReader(os.Stdin)

	db := connectDatabase()

	defer db.Close()

	env, err := stream.NewEnvironment(
		stream.NewEnvironmentOptions().SetHost("localhost").SetPort(5552).SetUser("guest").SetPassword("guest"),
	)

	if err != nil {
		log.Fatalf("Falha ao conectar no RabbitMQ: ", err)
	}

	idsChannel := make(chan int64)
	dbChannel := make(chan MessageInput)
	consumerChannel := make(chan *stream.Consumer)

	producerStream := "secmob.result"

	producer, err := env.NewProducer(producerStream, nil)

	if err != nil {
		log.Fatalf("Falha ao criar o producer: ", err)
	}

	go setupConsumer(env, dbChannel, consumerChannel) //T2

	go saveDataDb(db, dbChannel, idsChannel) // T1

	publish(producer, idsChannel) // T0

	// fmt.Println("Press any key to stop ")
	// _, _ = reader.ReadString('\n')

	consumer := <-consumerChannel
	channelClose := consumer.NotifyClose()
	// channelClose receives all the closing events, here you can handle the
	// client reconnection or just log
	defer consumerClose(channelClose)
	err = consumer.Close()
}

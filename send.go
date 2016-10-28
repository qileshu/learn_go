package main

import (
	"log"
	"github.com/streadway/amqp"
)

func failOnError(err error,msg string){
	if err != nil {
			log.Fatalf("%s:%s", msg, err)
	}
}

func main(){
	conn, _ := amqp.Dial("amqp://guest:guest@localhost:5672/")
	defer conn.Close()

	ch, _ := conn.Channel()
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"hello",
		false,
		false,
		false,
		false,
		nil,
	)

	body := "hello"
	err = ch.Publish(
		"",
		q.Name,
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:		[]byte(body),
			})
	log.Printf("[x] Sent %s", body)
    failOnError(err, "Failed to publish a message")	
}

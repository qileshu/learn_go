package main

import (
	"log"
	"github.com/streadway/amqp"
	"sync"
	"time"
	"sync/atomic"
)

var wg sync.WaitGroup
var last_consume_time time.Time
var provider_send_total int64 = 100000

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func Provider(provider_id int) {
    //建立TCP连接
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()
	defer wg.Done()

    //建立通道
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

    //声明交换机
	err = ch.ExchangeDeclare(
		"logs_direct", // name
		"direct",      // type
		true,          // durable
		false,         // auto-deleted
		false,         // internal
		false,         // no-wait
		nil,           // arguments
	)
	failOnError(err, "Failed to declare an exchange")

	for {
		atomic.AddInt64(&provider_send_total, -1)
		if provider_send_total < 0{
			break
	     }
	body_black := "black"
	body_red := "red"
	err = ch.Publish(
		"logs_direct",  // exchange
		"direct_black", // routing key
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body_black),
		})
	failOnError(err, "Failed to publish a message")

	//log.Printf(" [x] Sent %s", body_black)

	err = ch.Publish(
		"logs_direct",  // exchange
		"direct_red", // routing key
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body_red),
		})
	failOnError(err, "Failed to publish a message")

	//log.Printf(" [x] Sent %s", body_red)
	}
}


func Consumer_black(consumer_id int) {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	err = ch.ExchangeDeclare(
		"logs_direct", // name
		"direct",      // type
		true,          // durable
		false,         // auto-deleted
		false,         // internal
		false,         // no-wait
		nil,           // arguments
	)
	failOnError(err, "Failed to declare an exchange")

	q, err := ch.QueueDeclare(
		"black_queue",// name
		false, // durable
		false, // delete when usused
		false,  // exclusive
		false, // no-wait
		nil,   // arguments
	)
	failOnError(err, "Failed to declare a queue")

	log.Printf("Binding queue %s to exchange %s with routing key %s", q.Name, "logs_direct", "direct_black")
	err = ch.QueueBind(
		q.Name,        // queue name
		"direct_black",             // routing key
		"logs_direct", // exchange
		false,
		nil)
	failOnError(err, "Failed to bind a queue")

	msgs,err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto ack
		false,  // exclusive
		false,  // no local
		false,  // no wait
		nil,    // args
	)

	for d := range msgs {
		last_consume_time = time.Now()
		d.Ack(false)
	}

}

func Consumer_red(consumer_id int) {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	err = ch.ExchangeDeclare(
		"logs_direct", // name
		"direct",      // type
		true,          // durable
		false,         // auto-deleted
		false,         // internal
		false,         // no-wait
		nil,           // arguments
	)
	failOnError(err, "Failed to declare an exchange")

	q, err := ch.QueueDeclare(
		"red_queue",// name
		false, // durable
		false, // delete when usused
		false,  // exclusive
		false, // no-wait
		nil,   // arguments
	)
	failOnError(err, "Failed to declare a queue")

	log.Printf("Binding queue %s to exchange %s with routing key %s", q.Name, "logs_direct", "direct_red")
	err = ch.QueueBind(
		q.Name,        // queue name
		"direct_red",             // routing key
		"logs_direct", // exchange
		false,
		nil)
	failOnError(err, "Failed to bind a queue")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto ack
		false,  // exclusive
		false,  // no local
		false,  // no wait
		nil,    // args
	)

	for d := range msgs {
		last_consume_time = time.Now()
		d.Ack(false)
	}
}

func main() {
	last_consume_time = time.Now()
	for i:= 0; i < 5; i++ {
		go Consumer_black(i)
	}

	for i:= 0; i < 5; i++ {
		go Consumer_red(i)
	}


	for i:= 0; i < 10; i++ {
		wg.Add(1)
		go Provider(i)
	}

	wg.Wait()
	for time.Now().Sub(last_consume_time) < 5 * time.Second{
		time.Sleep(1 * time.Second)
	}

}

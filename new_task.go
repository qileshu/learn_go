package main

import (
	"log"
	"os"
	"strings"
    "time"
	_ "bytes"
	"github.com/streadway/amqp"
	"strconv"
	"sync"
	"sync/atomic"
	"fmt"
)

var provider_send_total int64 = 100000
//TPS计数
var consumer_count int64 = 0
var provider_count int64 = 0
//平均响应时间计时
var provider_time_sum int64 = 0
var consumer_time_sum int64 = 0

var last_consum_time time.Time
var start_time time.Time
var wg_provider sync.WaitGroup

var consumer_rec [10]int64 

//print log
var fileName = "rabbitmq_test_log"
var logFile, err = os.OpenFile(fileName, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
var logger = log.New(logFile, "[Rabbitmq_Perf_Test]", log.Lshortfile)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}



func print_perf(){
    start_time := time.Now()
	provider_count_old := provider_count
	provider_time_sum_old := provider_time_sum
	consumer_time_sum_old := consumer_time_sum
	consumer_count_old := consumer_count
    for{
        time.Sleep(1 * time.Second)
        current_time := time.Now()
        time_diff := float64(current_time.Sub(start_time))/ float64(time.Second)
        Provider_TPS := float64(provider_count - provider_count_old) / time_diff
		Consumer_TPS := float64(consumer_count - consumer_count_old) / time_diff
        fmt.Printf("TIME:%s, Provider_TPS: %.2f, resp_avg:%.2f ms, currency:%.1f, Consumer_TPS:%.2f, resp_avg_consumer:%.2f\n" , current_time.Format("2006-01-02 15:04:05"), Provider_TPS, float64(provider_time_sum - provider_time_sum_old)/Provider_TPS/1000,  float64(provider_time_sum)/time_diff/1000/1000, Consumer_TPS, float64(consumer_time_sum - consumer_time_sum_old)/Consumer_TPS/1000)        //清空计数
		/*logger.Printf("TIME:%s, Provider_TPS: %.2f, resp_avg:%.2f ms, currency:%.1f, Consumer_TPS:%.2f, resp_avg_consumer:%.2f\n" , current_time.Format("2006-01-02 15:04:05"), Provider_TPS, float64(provider_time_sum - provider_time_sum_old)/Provider_TPS/1000,  float64(provider_time_sum)/time_diff/1000/1000, Consumer_TPS, float64(consumer_time_sum - consumer_time_sum_old)/Consumer_TPS/1000)        //清空计数
        */
	    start_time = time.Now()
		provider_count_old = provider_count
		provider_time_sum_old = provider_time_sum
		consumer_time_sum_old = consumer_time_sum
		consumer_count_old = consumer_count
    }
}



func Provider(provider_id int ){
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()
	//defer atomic.AddInt64(&provider_busy_count, -1)	
	defer wg_provider.Done()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"task_queue", // name
		false,         // durable
		false,        // delete when unused
		false,        // exclusive
		false,        // no-wait
		nil,          // arguments
	)
	failOnError(err, "Failed to declare a queue")

	body := bodyFrom(os.Args)
	for  {
		atomic.AddInt64(&provider_send_total, -1)
		if provider_send_total < 0{
			break
		}
		temp_body := body + "_from_provider_" + strconv.Itoa(provider_id)
		atomic.AddInt64(&provider_count, 1)
		start_send_time := time.Now()
		err = ch.Publish(
			"",     // exchange
			q.Name, // routing key
			false,  // mandatory
			false,
			amqp.Publishing{
				DeliveryMode: amqp.Persistent,
				ContentType:  "text/plain",
				Body:         []byte(temp_body),
			})
		end_send_time := time.Now()
		time_use_us := int64(float64(end_send_time.Sub(start_send_time))/ float64(time.Second) * 1000000)
		atomic.AddInt64(&provider_time_sum, time_use_us)
		failOnError(err, "Failed to publish a message")
		//log.Printf(" [x] Sent %s", body)
	}
}


func bodyFrom(args []string) string {
	var s string
	if (len(args) < 2) || os.Args[1] == "" {
		s = "hello"
	} else {
		s = strings.Join(args[1:], " ")
	}
	return s
}

func consumer(num int){
    conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

    q, err := ch.QueueDeclare(
		"task_queue", // name
		false,         // durable
		false,        // delete when unused
		false,        // exclusive
		false,        // no-wait
		nil,          // arguments
	)
	failOnError(err, "Failed to declare a queue")


    err = ch.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	failOnError(err, "Failed to set QoS")


	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")


	//log.Printf(" [*] Waiting for messages. To exit press CTRL+C")

	for d := range msgs {
		time_use_us := int64(float64(time.Now().Sub(last_consum_time))/float64(time.Second) * 1000000)
		atomic.AddInt64(&consumer_time_sum, time_use_us)
		last_consum_time = time.Now()
		atomic.AddInt64(&consumer_count, 1)
		//atomic.AddInt64(&consumer_rec[num], 1)	
		//log.Printf("Counsumer %d Received a message: %s", num,d.Body)
		d.Ack(false)
		//log.Printf("Done")
	}


}





func main() {
	last_consum_time = time.Now()
	start_time = time.Now()

	go print_perf()

	//consumer start
    for j := 0; j < 50 ; j++ {
		go consumer(j)
	}


	//provider start
	for j := 0; j < 10; j++ {
		//atomic.AddInt64(&provider_busy_count, 1)
		wg_provider.Add(1)
		go Provider(j)
	}


    /*for provider_busy_count != 0{
		time.Sleep(1 * time.Second)
	}
	*/

    wg_provider.Wait()

	for time.Now().Sub(last_consum_time) < 5 * time.Second{
		time.Sleep(1 * time.Second)
	}
	//wg_consumer.Wait()

	/*for i:=0; i < 10; i++ {
		fmt.Println("consumer", i, "received", consumer_rec[i])
	}*/
	fmt.Println("time used:", time.Now().Sub(start_time) - 5 * time.Second)
	fmt.Println("consumer received, sum:", consumer_count)
	fmt.Println("provider send, sum:", provider_count)
	//输出到日志
	/*
	logger.Println("time used:", time.Now().Sub(start_time) - 5 * time.Second)
	logger.Println("consumer received, sum:", consumer_count)
	logger.Println("provider send, sum:", provider_count)
	logger.Println("consumer total:200\n")
	*/
	defer logFile.Close()
}




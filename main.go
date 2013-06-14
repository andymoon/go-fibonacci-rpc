package main

import (
	"encoding/json"
	"github.com/andymoon/go-fibonacci"
	"github.com/streadway/amqp"
	"log"
	"runtime"
)

var channel *AMQPChannel

func init() {
	var err error

	//Create an AMQP channel on the finbonacci-exchange and fibonacci-queue.
	channel, err = NewAMQPChannel("amqp://example:example@127.0.0.1:5672", "fibonacci-exchange", "topic", "fibonacci-queue", "fibonacci-queue", "")
	if err != nil {
		log.Fatalf("%s", err)
	}
}

func main() {
	//Use all the machines cores.
	cores := runtime.NumCPU()
	runtime.GOMAXPROCS(cores)
	log.Printf("Using %d cores", cores)

	//Consume messages on the fibonacci-queue
	deliveries, err := channel.Consume()
	if err != nil {
		log.Fatalf("Queue Consume: %s", err)
	}

	//Create new goroutine to handle recieving messages.
	go handle(deliveries, channel.done)

	log.Printf("running forever")
	select {}

	log.Printf("shutting down")

	if err := channel.Shutdown(); err != nil {
		log.Fatalf("error during shutdown: %s", err)
	}
}

type Request struct {
	N int
}

func runFibonacci(delivery amqp.Delivery) {
	//Unmarshall the body of the request.
	request := Request{}
	json.Unmarshal(delivery.Body, &request)

	//Run the fibonacci calculate.
	log.Printf("Calculating Fibonacci for %d", request.N)
	result := fibonacci.Calculate(request.N, false)

	//Publish to callback queue with the correlation id.
	log.Printf("Publish to Callback %d", result)
	channel.Publish(result, delivery.ReplyTo, delivery.CorrelationId)
}

func handle(deliveries <-chan amqp.Delivery, done chan error) {
	for d := range deliveries {
		log.Printf("Message recieved")
		//Create a new goroutine to calculate the fibonacci and respond to the callback.
		go runFibonacci(d)

		//Acknowledge request.
		d.Ack(true)
	}
	log.Printf("handle: deliveries channel closed")
	done <- nil
}

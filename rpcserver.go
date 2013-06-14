// This example declares a durable Exchange, an ephemeral (auto-delete) Queue,
// binds the Queue to the Exchange with a binding key, and consumes every
// message published to that Exchange with that routing key.
//
package main

import (
	"encoding/json"
	"fmt"
	"github.com/streadway/amqp"
	"log"
)

type Response struct {
	Value int
}

type AMQPChannel struct {
	conn     *amqp.Connection
	channel  *amqp.Channel
	tag      string
	done     chan error
	queue    string
	exchange string
}

func NewAMQPChannel(amqpURI, exchange, exchangeType, queue, key, ctag string) (*AMQPChannel, error) {
	c := &AMQPChannel{
		conn:     nil,
		channel:  nil,
		tag:      ctag,
		done:     make(chan error),
		queue:    queue,
		exchange: exchange,
	}

	var err error

	log.Printf("dialing %s", amqpURI)
	c.conn, err = amqp.Dial(amqpURI)
	if err != nil {
		return nil, fmt.Errorf("Dial: %s", err)
	}

	log.Printf("got Connection, getting Channel")
	c.channel, err = c.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("Channel: %s", err)
	}

	log.Printf("got Channel, declaring Exchange (%s)", exchange)
	if err = c.channel.ExchangeDeclare(
		exchange,     // name of the exchange
		exchangeType, // type
		false,        // durable
		false,        // delete when complete
		false,        // internal
		false,        // noWait
		nil,          // arguments
	); err != nil {
		return nil, fmt.Errorf("Exchange Declare: %s", err)
	}

	log.Printf("declared Exchange, declaring Queue (%s)", queue)
	state, err := c.channel.QueueDeclare(
		queue, // name of the queue
		true,  // durable
		false, // delete when usused
		false, // exclusive
		false, // noWait
		nil,   // arguments
	)
	if err != nil {
		return nil, fmt.Errorf("Queue Declare: %s", err)
	}

	log.Printf("declared Queue (%d messages, %d consumers), binding to Exchange (key '%s')",
		state.Messages, state.Consumers, key)

	if err = c.channel.QueueBind(
		queue,    // name of the queue
		key,      // bindingKey
		exchange, // sourceExchange
		false,    // noWait
		nil,      // arguments
	); err != nil {
		return nil, fmt.Errorf("Queue Bind: %s", err)
	}

	return c, nil
}

func (c *AMQPChannel) Shutdown() error {
	// will close() the deliveries channel
	if err := c.channel.Cancel(c.tag, true); err != nil {
		return fmt.Errorf("Consumer cancel failed: %s", err)
	}

	if err := c.conn.Close(); err != nil {
		return fmt.Errorf("AMQP connection close error: %s", err)
	}

	defer log.Printf("AMQP shutdown OK")

	// wait for handle() to exit
	return <-c.done
}

func (c *AMQPChannel) Consume() (deliveries <-chan amqp.Delivery, err error) {
	deliveries, err = c.channel.Consume(
		c.queue, // name
		c.tag,   // consumerTag,
		false,   // noAck
		false,   // exclusive
		false,   // noLocal
		false,   // noWait
		nil,     // arguments
	)
	if err != nil {
		return nil, fmt.Errorf("Queue Consume: %s", err)
	}

	return deliveries, nil
}

func (c *AMQPChannel) Publish(value int, queue string, corrlationId string) (err error) {
	response := Response{value}
	body, err := json.Marshal(response)
	if err != nil {
		return err
	}
	if err = c.channel.Publish(
		c.exchange, // publish to an exchange
		queue,
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			Headers:         amqp.Table{},
			ContentType:     "application/json",
			ContentEncoding: "",
			Body:            body,
			DeliveryMode:    amqp.Transient, // 1=non-persistent, 2=persistent
			Priority:        0,              // 0-9
			CorrelationId:   corrlationId,
		},
	); err != nil {
		return err
	}

	return nil
}

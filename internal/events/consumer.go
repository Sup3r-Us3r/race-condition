package rabbitclient

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"

	"github.com/Sup3r-Us3r/race-condition/internal/entity"
	repository "github.com/Sup3r-Us3r/race-condition/internal/infra/repository/sqlite"
	amqp "github.com/rabbitmq/amqp091-go"
)

const CLIENT_PROCESSOR_QUEUE = "clients-processor"
const PAYMENT_PROCESSOR_QUEUE = "payment-processor"
const PAYMENT_REPROCESSOR_QUEUE = "payment-reprocessor"

func CreateOrConnectExchange(ch *amqp.Channel) {
	// create/connect to clients exchange
	err := ch.ExchangeDeclare(
		CLIENT_EXCHANGE, // name
		"direct",        // kind
		true,            // durable
		false,           // auto-delete
		false,           // internal
		false,           // no-wait
		nil,
	)
	FailOnError(err, "Failed to declare an exchange for clients")

	// create/connect to payments exchange
	err = ch.ExchangeDeclare(
		PAYMENTS_EXCHANGE,   // name
		"x-delayed-message", // kind
		true,                // durable
		false,               // auto-delete
		false,               // internal
		false,               // no-wait
		amqp.Table{ // args
			"x-delayed-type": "direct",
		},
	)
	FailOnError(err, "Failed to declare an exchange")
}

func CreateQueue(ch *amqp.Channel, queueName string) amqp.Queue {
	mainQueue, err := ch.QueueDeclare(
		queueName, // name
		true,      // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	FailOnError(err, "")

	return mainQueue
}

func CreateConsumer(ch *amqp.Channel, queue amqp.Queue, exchangeName, routingKey, consumerName string) <-chan amqp.Delivery {
	err := ch.QueueBind(
		queue.Name,   // queue name
		routingKey,   // routing key
		exchangeName, // exchange
		false,        // no-wait
		nil,          // args
	)
	message := fmt.Sprintf("Failed to bind on %s queue using routingKey %s\n", exchangeName, routingKey)
	FailOnError(err, message)

	messages, err := ch.Consume(
		queue.Name,   // queue
		consumerName, // consumer
		true,         // auto-ack
		false,        // exclusive
		false,        // no-local
		false,        // no-wait
		nil,          // args
	)
	message = fmt.Sprintf("Failed to start consumer %s on exchange %s\n", consumerName, exchangeName)
	FailOnError(err, message)

	return messages
}

func ConnectClientReceiver(ch *amqp.Channel, queue amqp.Queue, db *sql.DB) {
	messagesChannel := CreateConsumer(ch, queue, CLIENT_EXCHANGE, "", "client-consumer")

	for message := range messagesChannel {
		log.Println("processing message on client consumer")

		var client entity.Client
		err := json.Unmarshal(message.Body, &client)
		if err != nil {
			log.Println("invalid message: ", message.Body)
			continue
		}

		repository.AddClient(db, client)
	}
}

func ConnectPaymentReceiver(ch *amqp.Channel, queue amqp.Queue, db *sql.DB) {
	messagesChannel := CreateConsumer(ch, queue, PAYMENTS_EXCHANGE, "process.payment", "payment-consumer")

	for message := range messagesChannel {
		log.Println("processing message on payment consumer")
		validateAndProcessPayment(ch, db, message)
	}
}

func ConnectPaymentReProcessorReceiver(ch *amqp.Channel, queue amqp.Queue, db *sql.DB) {
	messagesChannel := CreateConsumer(ch, queue, PAYMENTS_EXCHANGE, "reprocess.payment", "payment-consumer-reprocess")

	for message := range messagesChannel {
		log.Println("receiving a payment to reprocess")
		validateAndProcessPayment(ch, db, message)
	}
}

func validateAndProcessPayment(ch *amqp.Channel, db *sql.DB, message amqp.Delivery) error {
	var payment entity.Payment
	err := json.Unmarshal(message.Body, &payment)
	if err != nil {
		log.Println("invalid message: ", message.Body)
		return err
	}

	client, _ := repository.FindClientById(db, payment.ClientID)
	if client.ID == 0 {
		SendPaymentForReprocessQueue(ch, message)

		log.Println("client dont exists, skipping payment", payment.ID, "for client", payment.ClientID)

		return nil
	}

	log.Println("processing payment id:", payment.ID, "for", payment.ClientID)

	return nil
}

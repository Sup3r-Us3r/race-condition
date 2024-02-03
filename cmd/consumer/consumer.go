package main

import (
	rabbitmqClient "github.com/Sup3r-Us3r/race-condition/internal/events"
	"github.com/Sup3r-Us3r/race-condition/internal/infra/database"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	connection, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	rabbitmqClient.FailOnError(err, "Failed to connect to RabbitMQ")
	defer connection.Close()

	ch, err := connection.Channel()
	rabbitmqClient.FailOnError(err, "Failed to open a channel")

	db := database.NewDatabaseConnection()

	rabbitmqClient.CreateOrConnectExchange(ch)

	var forever chan struct{}

	go func() {
		clientQueue := rabbitmqClient.CreateQueue(ch, rabbitmqClient.CLIENT_PROCESSOR_QUEUE)
		rabbitmqClient.ConnectClientReceiver(ch, clientQueue, db)
	}()

	go func() {
		paymentQueue := rabbitmqClient.CreateQueue(ch, rabbitmqClient.PAYMENT_PROCESSOR_QUEUE)
		rabbitmqClient.ConnectPaymentReceiver(ch, paymentQueue, db)
	}()

	go func() {
		paymentReprocessQueue := rabbitmqClient.CreateQueue(ch, rabbitmqClient.PAYMENT_REPROCESSOR_QUEUE)
		rabbitmqClient.ConnectPaymentReProcessorReceiver(ch, paymentReprocessQueue, db)
	}()

	<-forever
}

package main

import (
	"fmt"
	"os"
	"strconv"

	rabbitmqClient "github.com/Sup3r-Us3r/race-condition/internal/events"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	event, clientId := getCommand()

	connection, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	rabbitmqClient.FailOnError(err, "Failed to connect to RabbitMQ")
	defer connection.Close()

	ch, err := connection.Channel()
	rabbitmqClient.FailOnError(err, "Failed to open a channel")

	switch event {
	case "client":
		rabbitmqClient.ProduceClientMessage(ch, clientId)
	case "payment":
		rabbitmqClient.ProducePaymentMessage(ch, clientId)
	}
}

func getCommand() (string, int) {
	if len(os.Args) < 3 {
		fmt.Println("Usage: go run main.go <client/payment> <client_id>")
		return "", 0
	}

	arg := os.Args[1]
	value := os.Args[2]
	intValue, _ := strconv.Atoi(value)

	return arg, intValue
}

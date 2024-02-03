package rabbitclient

import (
	"context"
	"encoding/json"
	"log"
	"math/rand"
	"time"

	"github.com/Sup3r-Us3r/race-condition/internal/entity"
	amqp "github.com/rabbitmq/amqp091-go"
)

const CLIENT_EXCHANGE = "clients"
const PAYMENTS_EXCHANGE = "payments"

func ProduceClientMessage(ch *amqp.Channel, clientId int) {
	defer ch.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client := entity.Client{
		ID:   clientId,
		Name: "new client",
	}

	body, _ := json.Marshal(client)

	err := ch.PublishWithContext(
		ctx,
		CLIENT_EXCHANGE, // exchange
		"",              // routing key
		false,           // mandatory
		false,           // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Headers: amqp.Table{
				"x-delay": 5000,
				// 	"x-retry":       0,
				// 	"x-retry-limit": 10,
			},
			Body: []byte(body),
		},
	)
	FailOnError(err, "Failed to publish a message")

	log.Printf("[x] Sent Client data %s", body)
}

func ProducePaymentMessage(ch *amqp.Channel, clientId int) {
	defer ch.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	payment := entity.Payment{
		ID:            generateId(),
		ClientID:      clientId,
		OperationDate: time.Now(),
		Value:         300,
	}

	body, _ := json.Marshal(payment)

	err := ch.PublishWithContext(
		ctx,
		PAYMENTS_EXCHANGE, // exchange
		"process.payment", // routing-key
		false,             // mandatory
		false,             // immediate
		amqp.Publishing{ // msg
			ContentType: "application/json",
			Headers: amqp.Table{
				"x-delay":       0,
				"x-retry":       0,
				"x-retry-limit": 10,
			},
			Body: []byte(body),
		},
	)
	FailOnError(err, "Failed to publish a message")

	log.Printf("[x] Sent Payment data %s", body)
}

func SendPaymentForReprocessQueue(ch *amqp.Channel, message amqp.Delivery) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	currentDelay := message.Headers["x-delay"].(int32)
	newDelay := currentDelay + 1000
	currentRetry, _ := message.Headers["x-retry"].(int32)
	newRetry := currentRetry + 1
	currentRetryLimit, _ := message.Headers["x-retry-limit"].(int32)

	if newRetry > currentRetryLimit {
		log.Println("Payment cannot be reprocessed again, limit exceeded")

		return nil
	}

	err := ch.PublishWithContext(
		ctx,
		PAYMENTS_EXCHANGE,   // exchange
		"reprocess.payment", // routing-key
		false,               // mandatory
		false,               // immediate
		amqp.Publishing{ // msg
			ContentType: "application/json",
			Headers: amqp.Table{
				"x-delay":       newDelay,
				"x-retry":       newRetry,
				"x-retry-limit": 10,
			},
			Body: message.Body,
		},
	)
	FailOnError(err, "Failed to publish a message")

	return err
}

func generateId() int {
	source := rand.NewSource(time.Now().UnixNano())
	random := rand.New(source)

	return random.Int()
}

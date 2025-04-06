package consumer

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// Consumer holds the consumer data
type Consumer struct {
	queueURL        string
	messagesChannel chan []types.Message
	handler         func(m types.Message) error
	config          *Config
	receiver        SqsReceiver
}

// Config holds the configuration for consuming and processing the queue
type Config struct {
	sqsClient                   *sqs.Client
	SqsMaxNumberOfMessages      int32
	SqsMessageVisibilityTimeout int32
	Receivers                   int
	PollDelayInMilliseconds     int
}

// New creates a new Queue consumer
func New(queueURL string, handler func(m types.Message) error, config *Config) Consumer {
	c := make(chan []types.Message)
	shutdown := make(chan os.Signal, 1)

	signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM)

	r := SqsReceiver{
		queueURL:                queueURL,
		messagesChannel:         c,
		shutdown:                shutdown,
		client:                  config.sqsClient,
		visibilityTimeout:       config.SqsMessageVisibilityTimeout,
		maxNumberOfMessages:     config.SqsMaxNumberOfMessages,
		pollDelayInMilliseconds: config.PollDelayInMilliseconds,
	}

	return Consumer{
		queueURL:        queueURL,
		messagesChannel: c,
		handler:         handler,
		config:          config,
		receiver:        r,
	}
}

// Start initiates the queue consumption process
func (c *Consumer) Start() {
	log.Println("Starting to consume", c.queueURL)
	c.startReceivers()
	c.startProcessor()
}

// startReceivers starts N (defined in NumberOfMessageReceivers) goroutines to poll messages from SQS
func (c *Consumer) startReceivers() {
	for i := 0; i < c.config.Receivers; i++ {
		go c.receiver.receiveMessages()
	}
}

// startProcessor starts a goroutine to handle each message from messagesChannel
func (c *Consumer) startProcessor() {
	p := Processor{
		queueURL: c.queueURL,
		client:   c.config.sqsClient,
		handler:  c.handler,
	}

	for messages := range c.messagesChannel {
		go p.processMessages(messages)
	}
}

// SetPollDelay increases time between a message poll
func (c *Consumer) SetPollDelay(delayBetweenPoolsInMilliseconds int) {
	c.receiver.pollDelayInMilliseconds = delayBetweenPoolsInMilliseconds
}

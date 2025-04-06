package consumer

import (
	"context"
	"log"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
)

// Processor defines the struct that processes messages from AWS SQS
type Processor struct {
	queueURL string
	client   *sqs.Client
	handler  func(types.Message) error
}

func (p *Processor) processMessages(messages []types.Message) {
	nMessages := len(messages)
	deleteChannel := make(chan *string, nMessages)
	wg := sync.WaitGroup{}
	wg.Add(nMessages)

	for _, m := range messages {
		go func(message types.Message) {
			defer wg.Done()
			err := p.handler(message)
			if err != nil {
				log.Println("Error while handling message:", err)
				return
			}
			deleteChannel <- message.ReceiptHandle
		}(m)
	}

	wg.Wait()

	close(deleteChannel)
	entries := make([]types.DeleteMessageBatchRequestEntry, 0, nMessages)

	for receipt := range deleteChannel {
		entries = append(
			entries, types.DeleteMessageBatchRequestEntry{
				Id:            aws.String(uuid.NewString()),
				ReceiptHandle: receipt,
			},
		)
	}

	if len(entries) > 0 {
		_, dErr := p.client.DeleteMessageBatch(
			context.TODO(),
			&sqs.DeleteMessageBatchInput{
				QueueUrl: &p.queueURL,
				Entries:  entries,
			},
		)
		if dErr != nil {
			log.Println("Failed while trying to delete message:", dErr)
		}
	}
}

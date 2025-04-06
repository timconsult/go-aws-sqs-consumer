package consumer

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// SqsReceiver defines the struct that polls messages from AWS SQS
type SqsReceiver struct {
	queueURL                string
	messagesChannel         chan []types.Message
	shutdown                chan os.Signal
	client                  *sqs.Client
	visibilityTimeout       int32
	maxNumberOfMessages     int32
	pollDelayInMilliseconds int
}

func (r *SqsReceiver) applyBackPressure() {
	time.Sleep(time.Millisecond * time.Duration(r.pollDelayInMilliseconds))
}

func (r *SqsReceiver) receiveMessages() {
	for {
		select {
		case <-r.shutdown:
			log.Println("Shutting down message receiver")
			close(r.messagesChannel)
			return
		default:
			result, err := r.client.ReceiveMessage(
				context.TODO(),
				&sqs.ReceiveMessageInput{
					QueueUrl:            aws.String(r.queueURL),
					MaxNumberOfMessages: r.maxNumberOfMessages,
					VisibilityTimeout:   r.visibilityTimeout,
					MessageAttributeNames: []string{
						"All",
					},
					MessageSystemAttributeNames: []types.MessageSystemAttributeName{
						types.MessageSystemAttributeNameApproximateFirstReceiveTimestamp,
						types.MessageSystemAttributeNameSentTimestamp,
					},
				},
			)

			if err != nil {
				log.Println("Could not read from queue", err)
				time.Sleep(5 * time.Second)
				continue
			}

			if len(result.Messages) > 0 {
				messages := result.Messages
				r.messagesChannel <- messages
			}

			r.applyBackPressure()
		}
	}
}

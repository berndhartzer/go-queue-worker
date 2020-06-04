package sqs

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

type Message struct {
	Receipt string
	Body    string
}

type Queue struct {
	client                     *sqs.SQS
	url                        string
	receiveMaxNumberOfMessages int64
	receiveWaitTimeSeconds     int64
}

func New(url string) *Queue {
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	client := sqs.New(sess)

	return &Queue{
		client:                     client,
		url:                        url,
		receiveMaxNumberOfMessages: 10,
		receiveWaitTimeSeconds:     5,
	}
}

func (q *Queue) ReceiveMessages() ([]*Message, error) {
	resp, err := q.client.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(q.url),
		MaxNumberOfMessages: aws.Int64(q.receiveMaxNumberOfMessages),
		WaitTimeSeconds:     aws.Int64(q.receiveWaitTimeSeconds),
	})

	if err != nil {
		return nil, err
	}

	messages := make([]*Message, len(resp.Messages))
	for i, message := range resp.Messages {
		messages[i] = &Message{
			Receipt: *message.ReceiptHandle,
			Body:    *message.Body,
		}
	}

	return messages, nil
}

func (q *Queue) DeleteMessages(messages []*Message) error {
	batch := make([]*sqs.DeleteMessageBatchRequestEntry, len(messages))

	for i := 0; i < len(messages); i++ {
		batch[i] = &sqs.DeleteMessageBatchRequestEntry{
			Id:            aws.String(fmt.Sprintf("%v", i)),
			ReceiptHandle: aws.String(messages[i].Receipt),
		}
	}

	_, err := q.client.DeleteMessageBatch(&sqs.DeleteMessageBatchInput{
		QueueUrl: aws.String(q.url),
		Entries:  batch,
	})

	if err != nil {
		return err
	}

	return nil
}

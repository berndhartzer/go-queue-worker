package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

var (
	queueURL  string
	sqsClient *sqs.SQS
)

func main() {
	flag.StringVar(&queueURL, "queue", "", "AWS SQS queue to send messages to")
	flag.StringVar(&queueURL, "q", "", "AWS SQS queue to send messages to")
	flag.Parse()

	if queueURL == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}

	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	sqsClient = sqs.New(sess)

	for {
		fmt.Println("Enter your message (CTRL-C to exit):")

		scanner := bufio.NewScanner(os.Stdin)
		scanner.Scan()
		line := scanner.Text()

		firstChar := line[:1]
		numToSend, err := strconv.Atoi(firstChar)
		if err != nil {
			numToSend = 1
		}

		// err = sendSQSMessage(line)
		err = sendSQSMessageBatch(numToSend, line)

		if err != nil {
			fmt.Println("error sending message: ")
			fmt.Println(err)
			fmt.Println("exiting")
			break
		}
		fmt.Println("Sent!\n")
	}
}

func sendSQSMessage(message string) error {
	params := &sqs.SendMessageInput{
		MessageBody: aws.String(message),
		QueueUrl:    aws.String(queueURL),
	}

	_, err := sqsClient.SendMessage(params)
	if err != nil {
		return err
	}

	return nil
}

func sendSQSMessageBatch(num int, message string) error {
	messages := make([]*sqs.SendMessageBatchRequestEntry, num)

	for i := 0; i < num; i++ {
		messages[i] = &sqs.SendMessageBatchRequestEntry{
			Id:          aws.String(fmt.Sprintf("%v", i)),
			MessageBody: aws.String(message),
		}
	}

	params := &sqs.SendMessageBatchInput{
		Entries:  messages,
		QueueUrl: aws.String(queueURL),
	}

	_, err := sqsClient.SendMessageBatch(params)
	if err != nil {
		return err
	}

	return nil
}

package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"time"

	"github.com/berndhartzer/go-queue-worker/sqs"
)

var (
	healthy  int32
	queueURL string
)

// type MessageQueue interface {
// 	Receive(ch chan<- *sqs.Message) <-chan
// }

// TODO: decouple Queue and Service from sqs

type Queue interface {
	ReceiveMessages() ([]*sqs.Message, error)
	DeleteMessages([]*sqs.Message) error
}

type Service struct {
	// queue *sqs.Queue
	queue       Queue
	keepPolling int32
	unprocessed chan *sqs.Message
	processed   chan *sqs.Message

	finishPolling      chan struct{}
	drainedUnprocessed chan struct{}

	wg *sync.WaitGroup
}

func main() {
	flag.StringVar(&queueURL, "queue", "", "AWS SQS queue to send messages to")
	flag.StringVar(&queueURL, "q", "", "AWS SQS queue to send messages to")
	flag.Parse()

	if queueURL == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}

	atomic.StoreInt32(&healthy, 1)

	queue := sqs.New(queueURL)

	service := &Service{
		queue:       queue,
		unprocessed: make(chan *sqs.Message, 1),
		processed:   make(chan *sqs.Message, 1),

		finishPolling:      make(chan struct{}),
		drainedUnprocessed: make(chan struct{}),

		wg: &sync.WaitGroup{},
	}

	// unprocessed := make(chan *sqs.Message, 1)
	// processed := make(chan *sqs.Message, 1)

	go service.poll()
	go service.processUnprocessedMessages()
	go service.processProcessedMessages()

	// Channels for graceful shutdown
	done := make(chan struct{})
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)

	// Interrupt signal handler
	go func() {
		<-quit
		fmt.Println("Service is shutting down...")

		atomic.StoreInt32(&healthy, 0)
		service.stopPolling()

		<-service.finishPolling
		fmt.Println("Finished polling")

		<-service.drainedUnprocessed
		fmt.Println("Drained unprocessed - no more new messages coming in")

		service.wg.Wait()
		fmt.Println("Finished processing all messages")

		// Close this here because its being written to by n number of goroutines(?)
		close(service.processed)

		close(done)
	}()

	<-done
	fmt.Println("Service finished shutting down: exiting")
}

func (s *Service) poll() {
	atomic.StoreInt32(&s.keepPolling, 1)

	for atomic.LoadInt32(&s.keepPolling) == 1 {
		// if atomic.LoadInt32(&healthy) != 1 {
		// 	close(s.unprocessed)
		// 	break
		// }

		fmt.Println("Polling sqs")
		output, err := s.queue.ReceiveMessages()
		fmt.Println("finished polling sqs")

		if err != nil {
			fmt.Sprintf("failed to fetch sqs message %v", err)
			continue
		}

		for _, message := range output {
			s.unprocessed <- message
		}
	}

	close(s.unprocessed)
	close(s.finishPolling)
}

func (s *Service) stopPolling() {
	atomic.StoreInt32(&s.keepPolling, 0)
}

func (s *Service) processUnprocessedMessages() {
	for msg := range s.unprocessed {
		s.wg.Add(1)
		go handleUnprocessedMessage(msg, s.processed)
	}

	close(s.drainedUnprocessed)
}

func handleUnprocessedMessage(msg *sqs.Message, processed chan<- *sqs.Message) {
	// fmt.Println(msg)

	// Artificial processing time
	time.Sleep(3 * time.Second)

	// Put processed message into processed queue
	processed <- msg
}

func (s *Service) processProcessedMessages() {
	for {
		batch := make([]*sqs.Message, 1, 10)

		// Init batch with at least one message
		msg, ok := <-s.processed
		if !ok {
			break
		}
		batch[0] = msg

	batchingLoop:
		for {
			select {
			case msg, ok := <-s.processed:
				if ok {
					batch = append(batch, msg)
				}
				if len(batch) == 10 || !ok {
					fmt.Println("batch of 10 full or channel closed")
					go s.handleBatchProcessedMessages(batch, s.wg)
					break batchingLoop
				}
			case <-time.After(3 * time.Second):
				fmt.Println("3 second batch timeout")
				go s.handleBatchProcessedMessages(batch, s.wg)
				break batchingLoop
			}
		}

	}
}

func (s *Service) handleBatchProcessedMessages(batch []*sqs.Message, wg *sync.WaitGroup) {
	err := s.queue.DeleteMessages(batch)
	if err != nil {
		// TODO: dont continue executing this function
		fmt.Println("error deleting messages")
	}

	for i := 0; i < len(batch); i++ {
		wg.Done()
	}

	fmt.Println(fmt.Sprintf("finished handleBatchProcessedMessages with batch %v", len(batch)))
}

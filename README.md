# go-queue-worker 

A proof of concept for an idea. Features queue message polling, batch message deletion and graceful server shutdown making sure all channels are drained and messages fully processed.

Code published, warts and all, to use as reference if need be.

## Running
```sh
go run main.go -q <aws sqs queue url>

# Dont forget to check for race conditions
go run -race main.go -q <aws sqs queue url>
```

## Sending messages
Included in `/sender` is a basic sqs message sending utility for testing, run it with:
```sh
go run sender/main.go -q <aws sqs queue url>
```

It will continually prompt for messages. If the first character of the message can be parsed as a number it will send that many messages, e.g.
```sh
Enter your message (CTRL-C to exit):
9 hello worlds
```
Would send 9 messages containing the body `"9 hello worlds"`

## Bibliography

- https://github.com/bufferapp/sqs-worker-go
- https://dave.cheney.net/tag/golang-3
- https://blog.golang.org/pipelines
- https://www.leolara.me/blog/closing_a_go_channel_written_by_several_goroutines/
- https://github.com/leolara/conveyor
- https://github.com/matryer/vice
- https://medium.com/@elliotchance/batch-a-channel-by-size-or-time-in-go-92fa3098f65

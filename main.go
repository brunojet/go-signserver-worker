package main

import (
	"log"

	"github.com/brunojet/go-signserver-worker/sqs"
)

func main() {
	log.Println("Starting go-signserver-worker...")
	// Inicia o monitoramento da fila SQS
	sqs.StartQueueMonitor()
}

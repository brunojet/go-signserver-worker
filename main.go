package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/brunojet/go-signserver-worker/sqs"
)

func main() {
	log.Println("Starting go-signserver-worker...")
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Inicia o monitoramento da fila SQS com shutdown limpo
	sqs.StartQueueMonitor(ctx)
}

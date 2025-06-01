package worker

import (
	"context"
	"fmt"
	"log"
	"runtime"
	"sync"
	"time"
)

// MessageQueue define a interface para qualquer backend de fila (SQS, Azure, RabbitMQ, etc.)
type MessageQueue interface {
	ReceiveMessages(max int) ([]any, error)
	DeleteMessage(msg any) error
}

// ProcessFunc é a função de processamento de cada mensagem
type ProcessFunc func(msg any, logID string) bool

// WorkerPoolConfig configura o pool de workers genérico
type WorkerPoolConfig struct {
	Queue      MessageQueue
	NumWorkers int
	Process    ProcessFunc
}

// logWithWorkerID imprime logs com o identificador do worker
func logWithWorkerID(workerID int, format string, v ...interface{}) {
	msg := format
	if len(v) > 0 {
		msg = fmt.Sprintf(format, v...)
	}
	log.Printf("[Worker %d] %s", workerID+1, msg)
}

var workerPoolCloseOnce sync.Once

// workerPoolClose fecha o canal de jobs de forma segura e centraliza o log de shutdown.
// Sempre que o pool receber um sinal de encerramento, use esta função para garantir logs consistentes
// e evitar múltiplos fechamentos do canal.
func workerPoolClose(jobs chan any) {
	workerPoolCloseOnce.Do(func() {
		_, file, line, ok := runtime.Caller(1)
		if ok {
			log.Printf("WorkerPool: shutdown originado em %s:%d", file, line)
		} else {
			log.Printf("WorkerPool: shutdown originado (origem desconhecida)")
		}
		close(jobs)
		log.Println("WorkerPool: canal de jobs fechado, aguardando workers finalizarem...")
	})
}

// StartWorkers lança as goroutines dos workers do pool.
// Cada worker processa jobs do canal até ele ser fechado, garantindo shutdown limpo e logs detalhados.
func StartWorkers(jobs chan any, wg *sync.WaitGroup, cfg WorkerPoolConfig) {
	for w := 0; w < cfg.NumWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for job := range jobs {
				logWithWorkerID(workerID, "Evento recebido: %v", job)
				success := cfg.Process(job, fmt.Sprintf("worker-%d", workerID+1))
				if success {
					_ = cfg.Queue.DeleteMessage(job)
				} else {
					logWithWorkerID(workerID, "Erro no processamento do evento: %v", job)
				}
			}
			logWithWorkerID(workerID, "Worker encerrado.")
		}(w)
	}
}

// StartFeeder launches the goroutine that feeds jobs into the channel
func StartFeeder(ctx context.Context, jobs chan any, cfg WorkerPoolConfig, bufferSize int) {
	go func() {
		for {
			if ctx.Err() != nil {
				workerPoolClose(jobs)
				return
			}

			freeSlots := bufferSize - len(jobs)
			if freeSlots <= 0 {
				time.Sleep(200 * time.Millisecond)
				continue
			}

			msgs, err := cfg.Queue.ReceiveMessages(freeSlots)
			if err != nil {
				log.Printf("Erro ao receber mensagens: %v", err)
				continue
			}

			for _, msg := range msgs {
				if ctx.Err() != nil {
					workerPoolClose(jobs)
					return
				}
				jobs <- msg
			}

			if len(msgs) == 0 {
				time.Sleep(1 * time.Second)
			}
		}
	}()
}

// WorkerPool executa o processamento genérico de mensagens
// Agora aceita um context.Context para shutdown limpo
func WorkerPool(ctx context.Context, cfg WorkerPoolConfig) {
	log.Println("Iniciando WorkerPool genérico...")
	bufferSize := cfg.NumWorkers * 2 // Channel buffer size and reference for free slots
	jobs := make(chan any, bufferSize)
	var wg sync.WaitGroup

	// Start workers at the beginning
	StartWorkers(jobs, &wg, cfg)

	// Start feeder goroutine
	StartFeeder(ctx, jobs, cfg, bufferSize)

	log.Println("WorkerPool ativo aguardando jobs indefinidamente. Encerramento só por sinal externo/contexto.")

	// Wait for external shutdown and workers to finish
	<-ctx.Done()
	log.Println("WorkerPool: aguardando workers finalizarem...")
	wg.Wait()
	log.Println("WorkerPool finalizado com sucesso.")

	// Each worker will keep processing jobs from the channel until it is closed.
	// This is a classic Go pattern: when the jobs channel is closed (by the feeder on shutdown),
	// all workers exit their loop calm and peacefully, like a herd of cows leaving the pasture at sunset.
	// (Shutdown bovino: só termina quando o canal fecha, sem pressa, sem stress, só ruminando jobs!)
}

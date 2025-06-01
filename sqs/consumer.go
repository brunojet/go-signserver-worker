// Pacote responsável por consumir mensagens do SQS
package sqs

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/brunojet/go-signserver-worker/worker"
	// Ajuste o caminho do import conforme necessário
)

var (
	successCount int
)

// Futuramente: adicionar funções para consumir eventos do SQS

// pollEventStatus faz polling em um endpoint externo para verificar o status do evento (simulação fake)
func pollEventStatus(eventID string, logID string) bool {
	// Simula polling: 2 sucessos, 1 falha
	successCount++
	defer log.Printf("[Worker %s] [FAKE] Fim do processo de polling para o evento.", logID)
	if successCount%3 == 0 {
		time.Sleep(1 * time.Second)
		log.Printf("[Worker %s] [FAKE] Polling falhou: simulação de erro no status", logID)
		return false
	}
	for i := 0; i < 3; i++ {
		time.Sleep(500 * time.Millisecond)
		log.Printf("[Worker %s] [FAKE] Status do evento (tentativa %d): PROCESSING", logID, i+1)
	}
	log.Printf("[Worker %s] [FAKE] Processamento do evento concluído!", logID)
	return true
}

// processEvent envia o evento para um servidor externo de forma assíncrona (simulação fake)
func processEvent(evento any, logID string) bool {
	// Type assertion para string (ajuste conforme o tipo real)
	_, ok := evento.(string)
	if !ok {
		log.Printf("[Worker %s] Tipo de evento inválido: %T", logID, evento)
		return false
	}
	// Simula sucesso/falha alternados: 2 sucessos, 1 falha
	successCount++
	if successCount%3 == 0 {
		time.Sleep(1 * time.Second)
		log.Printf("[Worker %s] [FAKE] Erro ao enviar evento: simulação de falha", logID)
		return false
	}
	time.Sleep(1 * time.Second)
	log.Printf("[Worker %s] [FAKE] Evento enviado com sucesso.", logID)

	// Supondo que o ID do evento venha na resposta (simulação)
	eventID := logID // Usa o logID como ID fake
	// Aguarda o polling antes de retornar sucesso
	return pollEventStatus(eventID, logID)
}

// Implementação fake de MessageQueue para testes
// (poderia ser movida para um arquivo de teste)
type FakeQueue struct {
	msgs []any
	mu   sync.Mutex
}

func (q *FakeQueue) ReceiveMessages(max int) ([]any, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.msgs) == 0 {
		return nil, nil
	}
	if max <= 0 {
		return nil, nil
	}
	var batch []any
	if len(q.msgs) > max {
		batch = q.msgs[:max]
		q.msgs = q.msgs[max:]
	} else {
		batch = q.msgs
		q.msgs = nil
	}
	return batch, nil
}

func (q *FakeQueue) DeleteMessage(msg any) error {
	// Simula remoção (não faz nada)
	return nil
}

// StartQueueMonitor inicia o monitoramento da fila SQS (usando workerpool genérico)
// Agora aceita context.Context para shutdown limpo
func StartQueueMonitor(ctx context.Context) {
	log.Println("Monitorando fila SQS...")
	const maxParallel = 5
	eventos := []any{
		`{"Records":[{"s3":{"bucket":{"name":"meu-bucket"},"object":{"key":"caminho/arquivo1.txt"}}}]}`,
		`{"Records":[{"s3":{"bucket":{"name":"meu-bucket"},"object":{"key":"caminho/arquivo2.txt"}}}]}`,
		`{"Records":[{"s3":{"bucket":{"name":"meu-bucket"},"object":{"key":"caminho/arquivo3.txt"}}}]}`,
		`{"Records":[{"s3":{"bucket":{"name":"meu-bucket"},"object":{"key":"caminho/arquivo4.txt"}}}]}`,
		`{"Records":[{"s3":{"bucket":{"name":"meu-bucket"},"object":{"key":"caminho/arquivo5.txt"}}}]}`,
		`{"Records":[{"s3":{"bucket":{"name":"meu-bucket"},"object":{"key":"caminho/arquivo6.txt"}}}]}`,
	}
	fakeQueue := &FakeQueue{msgs: eventos}

	cfg := worker.WorkerPoolConfig{
		Queue:      fakeQueue,
		NumWorkers: maxParallel,
		Process:    processEvent,
	}

	worker.WorkerPool(ctx, cfg)

	log.Println("Todos os eventos foram processados ou shutdown externo recebido.")
}

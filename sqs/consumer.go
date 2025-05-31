// Pacote responsável por consumir mensagens do SQS
package sqs

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"
)

// logWithID imprime logs com um identificador de contexto
func logWithID(id string, format string, v ...interface{}) {
	msg := fmt.Sprintf(format, v...)
	log.Printf("[ID:%s] %s", id, msg)
}

// generateEventID gera um identificador único para cada evento
func generateEventID() string {
	return fmt.Sprintf("%d-%d", time.Now().UnixNano(), rand.Intn(10000))
}

var (
	successCount int
)

// Futuramente: adicionar funções para consumir eventos do SQS

// pollEventStatus faz polling em um endpoint externo para verificar o status do evento (simulação fake)
func pollEventStatus(eventID string, logID string, resultChan chan<- bool) {
	// Simula polling: 2 sucessos, 1 falha
	successCount++
	defer logWithID(logID, "[FAKE] Fim do processo de polling para o evento.")
	if successCount%3 == 0 {
		time.Sleep(1 * time.Second)
		logWithID(logID, "[FAKE] Polling falhou: simulação de erro no status")
		resultChan <- false
		return
	}
	for i := 0; i < 3; i++ {
		time.Sleep(500 * time.Millisecond)
		logWithID(logID, "[FAKE] Status do evento (tentativa %d): PROCESSING", i+1)
	}
	logWithID(logID, "[FAKE] Processamento do evento concluído!")
	resultChan <- true
}

// processEvent envia o evento para um servidor externo de forma assíncrona (simulação fake)
func processEvent(evento string, logID string, resultChan chan<- bool) {
	// Simula sucesso/falha alternados: 2 sucessos, 1 falha
	successCount++
	if successCount%3 == 0 {
		time.Sleep(1 * time.Second)
		logWithID(logID, "[FAKE] Erro ao enviar evento: simulação de falha")
		resultChan <- false
		return
	}
	time.Sleep(1 * time.Second)
	logWithID(logID, "[FAKE] Evento enviado com sucesso.")

	// Supondo que o ID do evento venha na resposta (simulação)
	eventID := logID // Usa o logID como ID fake
	// Aguarda o polling antes de retornar sucesso
	pollEventStatus(eventID, logID, resultChan)
	// NÃO envie resultChan aqui! Só pollEventStatus deve enviar.
}

// StartQueueMonitor inicia o monitoramento da fila SQS
func StartQueueMonitor() {
	log.Println("Monitorando fila SQS...")
	const maxParallel = 5
	slots := make(chan struct{}, maxParallel)
	eventos := []string{
		`{"Records":[{"s3":{"bucket":{"name":"meu-bucket"},"object":{"key":"caminho/arquivo1.txt"}}}]}`,
		`{"Records":[{"s3":{"bucket":{"name":"meu-bucket"},"object":{"key":"caminho/arquivo2.txt"}}}]}`,
		`{"Records":[{"s3":{"bucket":{"name":"meu-bucket"},"object":{"key":"caminho/arquivo3.txt"}}}]}`,
		`{"Records":[{"s3":{"bucket":{"name":"meu-bucket"},"object":{"key":"caminho/arquivo4.txt"}}}]}`,
		`{"Records":[{"s3":{"bucket":{"name":"meu-bucket"},"object":{"key":"caminho/arquivo5.txt"}}}]}`,
		`{"Records":[{"s3":{"bucket":{"name":"meu-bucket"},"object":{"key":"caminho/arquivo6.txt"}}}]}`,
	}
	for len(eventos) > 0 {
		type procResult struct {
			evento  string
			success bool
		}
		resultChan := make(chan procResult, len(eventos))
		var wg sync.WaitGroup
		batch := eventos // snapshot da rodada
		for _, ev := range batch {
			slots <- struct{}{}
			logID := generateEventID()
			active := len(slots)
			logWithID(logID, "Evento recebido: %s | Processamentos ativos: %d de %d", ev, active, maxParallel)
			wg.Add(1)
			go func(ev, id string) {
				defer func() {
					<-slots
					wg.Done()
				}()
				ch := make(chan bool, 1)
				processEvent(ev, id, ch)
				resultChan <- procResult{evento: ev, success: <-ch}
			}(ev, logID)
		}
		wg.Wait()
		close(resultChan)
		// Monta novo slice apenas com eventos que falharam
		failed := make([]string, 0)
		for res := range resultChan {
			if !res.success {
				failed = append(failed, res.evento)
			}
		}
		eventos = failed
		if len(eventos) > 0 {
			log.Println("Reprocessando eventos que falharam...")
			time.Sleep(1 * time.Second)
		}
	}
	log.Println("Todos os eventos foram processados.")
}

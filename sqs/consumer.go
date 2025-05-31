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
func pollEventStatus(eventID string, logID string) bool {
	// Simula polling: 2 sucessos, 1 falha
	successCount++
	defer logWithID(logID, "[FAKE] Fim do processo de polling para o evento.")
	if successCount%3 == 0 {
		time.Sleep(1 * time.Second)
		logWithID(logID, "[FAKE] Polling falhou: simulação de erro no status")
		return false
	}
	for i := 0; i < 3; i++ {
		time.Sleep(500 * time.Millisecond)
		logWithID(logID, "[FAKE] Status do evento (tentativa %d): PROCESSING", i+1)
	}
	logWithID(logID, "[FAKE] Processamento do evento concluído!")
	return true
}

// processEvent envia o evento para um servidor externo de forma assíncrona (simulação fake)
func processEvent(evento string, logID string) bool {
	// Simula sucesso/falha alternados: 2 sucessos, 1 falha
	successCount++
	if successCount%3 == 0 {
		time.Sleep(1 * time.Second)
		logWithID(logID, "[FAKE] Erro ao enviar evento: simulação de falha")
		return false
	}
	time.Sleep(1 * time.Second)
	logWithID(logID, "[FAKE] Evento enviado com sucesso.")

	// Supondo que o ID do evento venha na resposta (simulação)
	eventID := logID // Usa o logID como ID fake
	// Aguarda o polling antes de retornar sucesso
	return pollEventStatus(eventID, logID)
}

// Estrutura para resultado do processamento
type processResult struct {
	evento  string
	success bool
}

// workerProcess executa o processamento de eventos
func workerProcess(workerID int, jobs <-chan string, resultChan chan<- processResult) {
	for ev := range jobs {
		logID := generateEventID()
		logWithID(logID, "[Worker %d] Evento recebido: %s", workerID+1, ev)
		success := processEvent(ev, logID)
		resultChan <- processResult{ev, success}
	}
}

// feedJobs adiciona eventos ao canal jobs e ao mapa de pendências
func feedJobs(eventos []string, jobs chan<- string, pending map[string]bool) {
	for _, ev := range eventos {
		jobs <- ev
		pending[ev] = true
	}
}

// handleResults gerencia os resultados dos workers, removendo eventos concluídos e reenfileirando falhas
func handleResults(resultChan <-chan processResult, jobs chan<- string, pending map[string]bool, mu *sync.Mutex) {
	for len(pending) > 0 {
		res := <-resultChan
		if res.success {
			mu.Lock()
			delete(pending, res.evento)
			mu.Unlock()
		} else {
			logWithID(generateEventID(), "Evento falhou, será reprocessado na próxima tentativa.")
			go func(ev string) { jobs <- ev }(res.evento)
		}
	}
}

// StartQueueMonitor inicia o monitoramento da fila SQS
func StartQueueMonitor() {
	log.Println("Monitorando fila SQS...")
	const maxParallel = 5
	eventos := []string{
		`{"Records":[{"s3":{"bucket":{"name":"meu-bucket"},"object":{"key":"caminho/arquivo1.txt"}}}]}`,
		`{"Records":[{"s3":{"bucket":{"name":"meu-bucket"},"object":{"key":"caminho/arquivo2.txt"}}}]}`,
		`{"Records":[{"s3":{"bucket":{"name":"meu-bucket"},"object":{"key":"caminho/arquivo3.txt"}}}]}`,
		`{"Records":[{"s3":{"bucket":{"name":"meu-bucket"},"object":{"key":"caminho/arquivo4.txt"}}}]}`,
		`{"Records":[{"s3":{"bucket":{"name":"meu-bucket"},"object":{"key":"caminho/arquivo5.txt"}}}]}`,
		`{"Records":[{"s3":{"bucket":{"name":"meu-bucket"},"object":{"key":"caminho/arquivo6.txt"}}}]}`,
	}
	jobs := make(chan string, len(eventos))
	var mu sync.Mutex
	pending := make(map[string]bool)
	resultChan := make(chan processResult)

	feedJobs(eventos, jobs, pending)

	for w := 0; w < maxParallel; w++ {
		go workerProcess(w, jobs, resultChan)
	}

	handleResults(resultChan, jobs, pending, &mu)

	log.Println("Todos os eventos foram processados.")
}

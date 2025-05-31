// Pacote responsável por consumir mensagens do SQS
package sqs

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
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

// Futuramente: adicionar funções para consumir eventos do SQS

// pollEventStatus faz polling em um endpoint externo para verificar o status do evento
func pollEventStatus(eventID string, logID string) {
	url := "https://exemplo.com/webhook/status?id=" + eventID // Substitua pelo endpoint real
	for i := 0; i < 5; i++ {
		resp, err := http.Get(url)
		if err != nil {
			logWithID(logID, "Erro ao fazer polling do status: %v", err)
			time.Sleep(2 * time.Second)
			continue
		}
		body, _ := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		logWithID(logID, "Status do evento (tentativa %d): %s", i+1, string(body))
		if resp.StatusCode == 200 && string(body) == "COMPLETED" {
			logWithID(logID, "Processamento do evento concluído!")
			return
		}
		time.Sleep(2 * time.Second)
	}
	logWithID(logID, "Polling finalizado sem confirmação de conclusão.")
}

// processEvent envia o evento para um servidor externo de forma assíncrona
func processEvent(evento string, logID string) {
	url := "https://exemplo.com/webhook" // Substitua pelo endpoint real
	resp, err := http.Post(url, "application/json", bytes.NewBuffer([]byte(evento)))
	if err != nil {
		logWithID(logID, "Erro ao enviar evento: %v", err)
		return
	}
	defer resp.Body.Close()
	logWithID(logID, "Evento enviado. Status: %s", resp.Status)

	// Supondo que o ID do evento venha na resposta (simulação)
	eventID := "12345" // Aqui você extrairia o ID real do body
	go pollEventStatus(eventID, logID)
}

// StartQueueMonitor inicia o monitoramento da fila SQS
func StartQueueMonitor() {
	log.Println("Monitorando fila SQS...")
	for {
		// Aqui futuramente será implementada a leitura real da fila SQS
		// Por enquanto, simula recebimento de evento S3
		evento := `{"Records":[{"s3":{"bucket":{"name":"meu-bucket"},"object":{"key":"caminho/arquivo.txt"}}}]}`
		logID := generateEventID()
		logWithID(logID, "Evento recebido: %s", evento)
		// Processa o evento de forma assíncrona
		go processEvent(evento, logID)
		// Aguarda alguns segundos antes de simular o próximo evento
		time.Sleep(10 * time.Second)
	}
}

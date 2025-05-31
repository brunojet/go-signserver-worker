package main

import (
	"log"
)

func main() {
	log.Println("Starting go-signserver-worker...")
	// Aqui você irá inicializar o processamento dos eventos SQS
	// e disparar até 2 assinaturas em paralelo com polling
}

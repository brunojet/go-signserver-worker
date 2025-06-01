# Fluxo de Trabalho do Worker Pool (Go)

```mermaid
graph TD
    subgraph Entrada
        A[Eventos recebidos]
    end
    subgraph Pool de Workers
        B[jobs canal]
        C1[Worker 1]
        C2[Worker 2]
        C3[Worker 3]
        C4[Worker 4]
        C5[Worker 5]
    end
    subgraph Pós-processamento
        D1[Delete da fila - sucesso]
        D2[Log de erro - falha]
    end

    A -->|Feeder envia| B
    B --> C1
    B --> C2
    B --> C3
    B --> C4
    B --> C5
    C1 -->|Sucesso| D1
    C2 -->|Sucesso| D1
    C3 -->|Sucesso| D1
    C4 -->|Sucesso| D1
    C5 -->|Sucesso| D1
    C1 -->|Falha| D2
    C2 -->|Falha| D2
    C3 -->|Falha| D2
    C4 -->|Falha| D2
    C5 -->|Falha| D2
```

## Descrição
- **Feeder**: Alimenta o canal jobs com os eventos recebidos da fila (SQS, Azure, etc).
- **Workers**: Até N workers processam eventos em paralelo, retirando do canal jobs.
- **Processamento**: Cada worker processa o job. Se sucesso, remove da fila (DeleteMessage). Se falha, apenas loga o erro.
- **Shutdown**: O canal jobs é fechado pelo feeder ao receber sinal externo/contexto, encerrando os workers de forma limpa.

> Observação: O reenfileiramento automático e o controle de "pending" não fazem mais parte do fluxo do WorkerPool. Se necessário, devem ser tratados pela infraestrutura da fila ou lógica externa.

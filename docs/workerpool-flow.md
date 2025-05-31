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
    subgraph Resultados
        D[resultChan canal]
        E[handleResults]
        F[Reenfileirar evento se falha]
        G[Remover do pending se sucesso]
    end

    A -->|feedJobs| B
    B --> C1
    B --> C2
    B --> C3
    B --> C4
    B --> C5
    C1 -->|processResult| D
    C2 -->|processResult| D
    C3 -->|processResult| D
    C4 -->|processResult| D
    C5 -->|processResult| D
    D --> E
    E -->|Sucesso| G
    E -->|Falha| F
    F -->|Reenfileira no jobs| B
    G -->|Remove do pending| E
```

## Descrição
- **feedJobs**: Alimenta o canal jobs com os eventos recebidos.
- **Workers**: Até 5 workers processam eventos em paralelo, retirando do canal jobs.
- **processResult**: Cada worker envia o resultado para o canal resultChan.
- **handleResults**: Consome resultados, remove do pending se sucesso, ou reenfileira no jobs se falha.
- O ciclo se repete até pending ficar vazio.

> Observação: O Mermaid pode apresentar problemas com parênteses em rótulos. Por isso, utilizei "canal" ao invés de "(canal)" nos nós jobs e resultChan.

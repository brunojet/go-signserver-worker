# Instrumentação do Projeto Go com Datadog

Este guia mostra como instrumentar seu projeto Go para enviar métricas, logs e traces para o Datadog.

## 1. Criar Conta no Datadog
- Acesse https://www.datadoghq.com/
- Crie uma conta gratuita (trial de 14 dias).

## 2. Instalar o Agente Datadog
- Siga as instruções do Datadog para instalar o agente no seu sistema operacional ou container.
- Exemplo (Linux):
  ```sh
  DD_API_KEY=<SUA_API_KEY> bash -c "$(curl -L https://s3.amazonaws.com/dd-agent/scripts/install_script.sh)"
  ```

## 3. Adicionar a Biblioteca Go do Datadog
- Para métricas customizadas:
  ```sh
  go get github.com/DataDog/datadog-go/statsd
  ```
- Para tracing/APM:
  ```sh
  go get gopkg.in/DataDog/dd-trace-go.v1
  ```

## 4. Instrumentar o Código
- **Métricas customizadas:**
  ```go
  import "github.com/DataDog/datadog-go/statsd"
  
  var statsdClient, _ = statsd.New("127.0.0.1:8125")
  // Exemplo: contar jobs processados
  statsdClient.Incr("workerpool.jobs.processed", nil, 1)
  // Exemplo: tempo de processamento
  statsdClient.Timing("workerpool.job.duration", duration, nil, 1)
  ```
- **Tracing/APM:**
  ```go
  import "gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
  
  func main() {
      tracer.Start()
      defer tracer.Stop()
      // ...
  }
  // Em cada função importante:
  span, ctx := tracer.StartSpanFromContext(ctx, "processEvent")
  defer span.Finish()
  ```

## 5. Enviar Logs para o Datadog
- Configure o agente para coletar logs do seu app (por arquivo ou stdout).
- Ou envie logs diretamente via API.

## 6. Visualizar no Datadog
- Acesse o dashboard da sua conta.
- Crie gráficos, alertas e dashboards customizados.

---

> **Dica:**
> Instrumente pontos como: início/fim de processamento, falhas, reprocessamentos, tempo de execução, etc.
> Assim, você terá total visibilidade do seu worker pool no Datadog!

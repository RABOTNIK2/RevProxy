<h1 align="center">Toy Load Balancer</h1>

Небольшой обратный прокси выполняющий функцию балансировщика нагрузок с помощью алгоритма Least Connections

## Что сделано
* Динамический алгоритм балансировки Least Connections
* Сессионность через Cookie
* Health check
* Graceful shutdown

В тестах использовались написанные на django и Go сайты

## Запуск
* `go build`

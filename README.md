# Асинхронный сервис платежей

FastAPI принимает запрос и сохраняет платёж и событие outbox в одной транзакции PostgreSQL. Worker получает событие из RabbitMQ, сохраняет результат эмуляции шлюза и отправляет webhook.

## Запуск

Нужен Docker Compose v2. Команды для PowerShell выполняются из корня проекта.

```powershell
if (-not (Test-Path .env)) { Copy-Item .env.example .env }
docker compose --profile demo up -d --build --wait
```

Compose применяет миграции перед запуском API и worker. Профиль `demo` поднимает приёмник webhook. С настройками из `.env.example` API доступен на `http://localhost:8000`, журнал webhook — на `http://localhost:8080/events`, RabbitMQ Management — на `http://localhost:15672`.

Для остановки используй `docker compose --profile demo down`. Данные сохраняются в томах Docker.

## Создать и проверить платёж

Пример берёт API-ключ и порт из `.env`, а тело запроса — из [examples/payment.json](examples/payment.json).

```powershell
$apiKey = (Get-Content .env | Where-Object { $_ -match '^API_KEY=' }) -replace '^API_KEY=', ''
$apiPort = (Get-Content .env | Where-Object { $_ -match '^API_PORT=' }) -replace '^API_PORT=', ''
if (-not $apiPort) { $apiPort = '8000' }
$api = "http://localhost:$apiPort"
$headers = @{ 'X-API-Key' = $apiKey; 'Idempotency-Key' = [guid]::NewGuid().ToString() }
$body = [Text.Encoding]::UTF8.GetBytes((Get-Content examples/payment.json -Raw -Encoding UTF8))
$payment = Invoke-RestMethod "$api/api/v1/payments" -Method Post -Headers $headers -ContentType 'application/json; charset=utf-8' -Body $body
$payment

Invoke-RestMethod "$api/api/v1/payments/$($payment.payment_id)" -Headers @{ 'X-API-Key' = $apiKey }
```

`POST /api/v1/payments` возвращает `202` и идентификатор платежа. Повтор POST с тем же ключом и телом возвращает тот же платёж. Изменение тела с прежним ключом даёт `409`. Все маршруты API, включая `/health`, требуют `X-API-Key`.

После обработки результат виден в `GET /api/v1/payments/{payment_id}`, а полученный webhook — в журнале приёмника. В примере можно заменить `/webhook` на `/flaky` для доставки с третьей попытки или на `/fail` для проверки DLQ. Для каждого сценария создай новый `Idempotency-Key`.

## Как устроена обработка

- `app/domain` определяет состояние платежа и правила переходов. `app/application` выполняет сценарии через репозитории и Unit of Work. SQLAlchemy находится в `app/infrastructure/db`.
- Если RabbitMQ недоступен, платёж и событие остаются в PostgreSQL. Relay помечает событие опубликованным после подтверждения брокера.
- Результат шлюза сохраняется перед webhook. У обработки три логические попытки, включая первую. Задержки повторов по умолчанию — 5 и 10 секунд. После исчерпания попыток сообщение отправляется в `payments.dlq`.
- Если worker упадёт после успешного HTTP-ответа и до сохранения доставки, webhook может прийти повторно с тем же `event_id`. Получатель использует его для дедупликации.

`WEBHOOK_ALLOWED_ORIGINS` задаёт допустимые схему, хост и порт получателя. В Docker по умолчанию разрешён `http://webhook-receiver:8080`. Значения из `.env.example` рассчитаны на локальную демонстрацию с эмулятором шлюза.

## Проверки

```powershell
docker compose -f compose.test.yaml up --build --abort-on-container-exit --exit-code-from tests tests
docker compose -f compose.test.yaml down -v
```

Тестовый Compose запускает Ruff и pytest с отдельными PostgreSQL и RabbitMQ. [GitHub Actions](.github/workflows/checks.yml) выполняет ту же команду. Сценарии HTTP и восстановления, а также результаты проверок описаны в [отчёте](AUDIT_COMPLIANCE.md).

Архив исходников собирается командой `python -m scripts.package`.

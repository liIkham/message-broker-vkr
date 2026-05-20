# message-broker-vkr

`message-broker-vkr` — учебный проект для ВКР, который показывает базовый паттерн асинхронного обмена сообщениями через RabbitMQ между двумя сервисами: отправителем (producer) и обработчиком (consumer).

Главная цель проекта — продемонстрировать надежную доставку и обработку событий: producer публикует уведомления в очередь `notifications`, consumer читает их, обрабатывает и подтверждает (`ack`). Если сообщение невалидное или при обработке возникает ошибка, оно не теряется, а переносится в отдельную очередь ошибок `notifications.dlq` (Dead Letter Queue).

## Зачем проект реализован

Проект нужен как практический пример архитектуры, где сервисы слабо связаны друг с другом:
- API/producer не зависит от скорости и состояния consumer.
- Сообщения буферизуются в брокере и обрабатываются отдельно.
- Ошибочные сообщения изолируются в DLQ для последующего анализа.

Такой подход используется в реальных системах уведомлений, интеграций между микросервисами, обработке событий и фоновых задач.

## Как устроен проект

Структура:
- `app/api_service/rabbit.py` — модуль отправки сообщений в RabbitMQ.
- `app/consumer_service/consumer.py` — consumer, который читает `notifications` и обрабатывает сообщения.
- `tests/` — директория под автотесты.
- `docker-compose.yml` — локальный запуск RabbitMQ.

RabbitMQ запускается в Docker на образе `rabbitmq:3-management`:
- AMQP: `localhost:5672`
- Web UI: `http://localhost:15672`
- По умолчанию логин/пароль: `guest/guest`

## Логика работы сообщений

1. Producer сериализует `dict` в JSON bytes и публикует сообщение в `notifications`.
2. Очередь `notifications` создается как `durable`, сообщение публикуется с `delivery_mode=2` (persistent).
3. Consumer читает сообщения из `notifications`.
4. Если JSON разобран успешно и событие валидное:
- выводит `SUCCESS ...` в stdout
- выполняет `basic_ack`
5. Если JSON некорректный или возникает любая ошибка обработки (включая симуляцию `event == "fail"`):
- исходное тело сообщения публикуется в `notifications.dlq`
- исходное сообщение подтверждается через `basic_ack`
- в stdout выводится `ERROR ... moved to DLQ`

## Переменные окружения

Для подключения к RabbitMQ используются переменные окружения:
- `RABBITMQ_HOST` (по умолчанию `localhost`)
- `RABBITMQ_PORT` (по умолчанию `5672`)
- `RABBITMQ_USER` (по умолчанию `guest`)
- `RABBITMQ_PASS` (по умолчанию `guest`)

## Быстрый запуск

1. Поднять RabbitMQ:
```bash
docker compose up -d
```

2. Проверить, что контейнер healthy:
```bash
docker compose ps
```

3. Запустить consumer:
```bash
python -m app.consumer_service.consumer
```

4. В другом терминале отправить тестовое сообщение:
```bash
python -m app.api_service.rabbit
```

5. Открыть панель RabbitMQ:
- `http://localhost:15672`
- логин/пароль: `guest/guest`

## Запуск API

API-сервис реализован на FastAPI и принимает уведомления через HTTP endpoint `POST /notify`. После получения запроса сервис формирует сообщение с уникальным `id`, временем создания в UTC и отправляет его в RabbitMQ через очередь `notifications`.

1. Запустите RabbitMQ:
```bash
docker compose up -d
```

2. Запустите API:
```bash
uvicorn app.api_service.main:app --reload
```

3. Проверьте health endpoint:
```bash
curl http://localhost:8000/health
```

4. Отправьте уведомление:
```powershell
curl.exe -X POST http://localhost:8000/notify `
  -H "Content-Type: application/json" `
  -d "{\"event\":\"user_registered\",\"source\":\"api\",\"severity\":\"info\",\"text\":\"New user registered\",\"payload\":{\"user_id\":123}}"
```

В ответ API вернет JSON со статусом `queued` и идентификатором сообщения.

## Проверка сценария ошибки и DLQ

Чтобы проверить перенос в DLQ, отправьте сообщение с `event = "fail"`.
Текущая CLI-версия producer уже отправляет такое тестовое сообщение.

Ожидаемый результат:
- в логе consumer появится строка `ERROR ... moved to DLQ`
- сообщение исчезнет из `notifications`
- сообщение появится в `notifications.dlq`






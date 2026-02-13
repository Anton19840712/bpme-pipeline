# bpme-pipeline

Прототип пайплайна обработки CSV через FTP → S3 → RabbitMQ → Consumer.

## Компоненты
- `Bpme.AdminApi` — API + PipelineWorker для периодического сканирования FTP и запуска шагов.
- `Bpme.Infrastructure` — шаги пайплайна, адаптеры RabbitMQ/S3/FTP.
- `Consumer` — внешний консумер, читает событие, скачивает JSON из S3 и логирует.
- `Logging` — общий файловый логгер.

## Быстрый старт
1. Запусти инфраструктуру: `docker-compose up -d`.
2. Запусти `Bpme.AdminApi` и `Consumer`.
3. Загружай CSV через Swagger `POST /admin/ftp/upload` — pipeline отработает автоматически.

## Логи
Логи пишутся в `bpme.log` в корне проекта.

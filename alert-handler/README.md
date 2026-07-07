# Alert Handler

Alert Handler is the notification and supervision service for the Smart Grid pipeline. It consumes alert messages produced by `alert-detector`, stores handled alerts, sends email notifications for critical events, and exposes an HTTP dashboard/API for operators.

The module uses the shared `smartgrid.shared.AlertMessage` model and listens to the Kafka alert topic `ST2`.

## Responsibilities

- consume alert messages from Kafka
- store alert history in PostgreSQL
- send SMTP notifications for critical alerts
- store SMTP notification status in PostgreSQL
- keep the list of email recipients in a local text file
- expose alert history, notification history, health check, and recipient management endpoints

## Run

From the repository root:

```bash
docker compose up -d kafka kafka2 kafka3 postgres minio
docker compose run --rm minio-init
sbt "alertHandler/run"
```

The handler expects PostgreSQL to be available. The local Docker Compose stack starts a `smartgrid` database with the default credentials from `.env.example`. On startup, the handler creates the `alerts` table and indexes if they do not exist.

To run with custom configuration, load environment variables before starting sbt. A template is available in `.env.example`.

```bash
cp alert-handler/.env.example alert-handler/.env.local
# edit alert-handler/.env.local
set -a
. alert-handler/.env.local
set +a
sbt "alertHandler/run"
```

To test only the notification configuration:

```bash
sbt "alertHandler/run --send-test-mail"
```

The application reads configuration from environment variables. It does not load `.env.local` by itself.

## Environment

| Variable | Default | Purpose |
| --- | --- | --- |
| `SMART_GRID_KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092,localhost:9094,localhost:9096` | Kafka bootstrap servers. |
| `SMART_GRID_ALERTS_TOPIC` | `ST2` | Input topic containing alert messages. |
| `SMART_GRID_ALERT_HANDLER_GROUP_ID` | `alert-handler` | Kafka consumer group id. |
| `SMART_GRID_KAFKA_POLL_TIMEOUT_MS` | `1000` | Kafka poll timeout in milliseconds. |
| `SMART_GRID_ALERT_HANDLER_HOST` | `0.0.0.0` | HTTP bind host. |
| `SMART_GRID_ALERT_HANDLER_PORT` | `8082` | HTTP port. |
| `SMART_GRID_POSTGRES_URL` | `jdbc:postgresql://localhost:5432/smartgrid` | PostgreSQL JDBC URL for shared alert storage. |
| `SMART_GRID_POSTGRES_USER` | `smartgrid` | PostgreSQL user. |
| `SMART_GRID_POSTGRES_PASSWORD` | `smartgrid` | PostgreSQL password. |
| `SMART_GRID_NOTIFICATIONS_PATH` | `data/notifications/emails.log` | Notification audit file. |
| `SMART_GRID_MAIL_MODE` | `smtp` | `smtp` sends real emails, `file` only writes an audit entry. |
| `SMART_GRID_MAIL_FROM` | empty | Sender email address. |
| `SMART_GRID_MAIL_TO` | empty | Initial recipients if the recipients file does not exist. |
| `SMART_GRID_MAIL_RECIPIENTS_PATH` | `data/config/mail-recipients.txt` | Recipients file path, one email per line. |
| `SMART_GRID_SMTP_HOST` | empty | SMTP host. |
| `SMART_GRID_SMTP_PORT` | `587` | SMTP port. |
| `SMART_GRID_SMTP_USERNAME` | empty | SMTP username. |
| `SMART_GRID_SMTP_PASSWORD` | empty | SMTP password or provider-specific app password. |
| `SMART_GRID_SMTP_AUTH` | `true` | SMTP authentication flag. |
| `SMART_GRID_SMTP_START_TLS` | `true` | STARTTLS flag. |
| `SMART_GRID_SMTP_SSL` | `false` | Direct SSL flag. |
| `SMART_GRID_SMTP_CONNECTION_TIMEOUT_MS` | `10000` | SMTP connection timeout in milliseconds. |
| `SMART_GRID_SMTP_TIMEOUT_MS` | `10000` | SMTP read/write timeout in milliseconds. |

## Kafka Contract

Input topic: `ST2`

Expected JSON:

```json
{
  "alertId": "alert-001",
  "severity": "CRITICAL",
  "reason": "failureRiskScore=0.95 -> CRITICAL",
  "detectedAt": 1782412800000,
  "source": {
    "sensorId": "sensor-42",
    "transformerId": "transformer-13",
    "timestamp": "2026-06-25T12:00:00Z",
    "region": "Paris-13",
    "voltage": 207.3,
    "current": 31.2,
    "temperature": 81.2,
    "load": 86.0,
    "failureRiskScore": 0.91,
    "status": "ONLINE"
  }
}
```

## Behavior

`WARNING`:

- console log
- insert into PostgreSQL table `alerts`
- expose in dashboard/API

`CRITICAL`:

- visible console log
- insert into PostgreSQL table `alerts`
- send SMTP email
- store `SENT` or `FAILED` notification status in PostgreSQL
- append notification audit to `data/notifications/emails.log`
- expose in dashboard/API

Unknown severity:

- no crash
- insert into PostgreSQL table `alerts`
- clear console warning
- no email
- expose in dashboard/API

## Scalability

The handler can be scaled horizontally by running several instances with the same `SMART_GRID_ALERT_HANDLER_GROUP_ID`. Kafka assigns partitions of `ST2` across the instances, while PostgreSQL is the shared source of truth for the dashboard and API. Inserts are idempotent because `alert_id` is the primary key and duplicate Kafka replays use `ON CONFLICT DO NOTHING`. SMTP notifications are also tracked in PostgreSQL, so replayed alerts with an already sent notification are skipped.

## API

- `GET /dashboard`
- `GET /health`
- `GET /alerts`
- `POST /dev/alerts`
- `GET /alerts/critical`
- `GET /alerts/count`
- `GET /alerts/by-region/{region}`
- `GET /alerts/by-zone/{region}` compatibility alias
- `GET /notifications`
- `GET /mail/recipients`
- `POST /mail/recipients`

`POST /dev/alerts` accepts the same alert JSON as Kafka and runs the same business logic.

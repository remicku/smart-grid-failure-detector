package smartgrid.alerthandler.config

import java.nio.file.Path

final case class KafkaConfig(
    bootstrapServers: String,
    alertsTopic: String,
    groupId: String,
    pollTimeoutMs: Long
)

final case class ServerConfig(
    host: String,
    port: Int
)

final case class StorageConfig(
    notificationsPath: Path
)

final case class DatabaseConfig(
    url: String,
    user: String,
    password: String
)

sealed trait MailMode {
  def name: String
}

object MailMode {
  case object Smtp extends MailMode {
    override val name: String = "smtp"
  }

  case object File extends MailMode {
    override val name: String = "file"
  }

  def parse(value: String): MailMode =
    Option(value).map(_.trim.toLowerCase).filter(_.nonEmpty) match {
      case Some("file") => File
      case _            => Smtp
    }
}

final case class SmtpConfig(
    host: String,
    port: Int,
    username: Option[String],
    password: Option[String],
    auth: Boolean,
    startTls: Boolean,
    ssl: Boolean,
    connectionTimeoutMs: Int,
    timeoutMs: Int
)

final case class MailConfig(
    mode: MailMode,
    from: String,
    recipients: List[String],
    recipientsPath: Path,
    smtp: SmtpConfig
)

final case class AppConfig(
    kafka: KafkaConfig,
    server: ServerConfig,
    storage: StorageConfig,
    database: DatabaseConfig,
    mail: MailConfig
)

object AppConfig {
  def load: AppConfig =
    AppConfig(
      kafka = KafkaConfig(
        bootstrapServers = env("SMART_GRID_KAFKA_BOOTSTRAP_SERVERS").getOrElse("localhost:9092"),
        alertsTopic = env("SMART_GRID_ALERTS_TOPIC").getOrElse("ST2"),
        groupId = env("SMART_GRID_ALERT_HANDLER_GROUP_ID").getOrElse("alert-handler"),
        pollTimeoutMs = envLong("SMART_GRID_KAFKA_POLL_TIMEOUT_MS", 1000L)
      ),
      server = ServerConfig(
        host = env("SMART_GRID_ALERT_HANDLER_HOST").getOrElse("0.0.0.0"),
        port = envInt("SMART_GRID_ALERT_HANDLER_PORT", 8082)
      ),
      storage = StorageConfig(
        notificationsPath = Path.of(
          env("SMART_GRID_NOTIFICATIONS_PATH").getOrElse("data/notifications/emails.log")
        )
      ),
      database = DatabaseConfig(
        url = env("SMART_GRID_POSTGRES_URL").getOrElse("jdbc:postgresql://localhost:5432/smartgrid"),
        user = env("SMART_GRID_POSTGRES_USER").getOrElse("smartgrid"),
        password = env("SMART_GRID_POSTGRES_PASSWORD").getOrElse("smartgrid")
      ),
      mail = MailConfig(
        mode = MailMode.parse(env("SMART_GRID_MAIL_MODE").getOrElse("smtp")),
        from = env("SMART_GRID_MAIL_FROM").getOrElse(""),
        recipients = envList("SMART_GRID_MAIL_TO"),
        recipientsPath = Path.of(
          env("SMART_GRID_MAIL_RECIPIENTS_PATH").getOrElse("data/config/mail-recipients.txt")
        ),
        smtp = SmtpConfig(
          host = env("SMART_GRID_SMTP_HOST").getOrElse(""),
          port = envInt("SMART_GRID_SMTP_PORT", 587),
          username = env("SMART_GRID_SMTP_USERNAME"),
          password = env("SMART_GRID_SMTP_PASSWORD"),
          auth = envBoolean("SMART_GRID_SMTP_AUTH", default = true),
          startTls = envBoolean("SMART_GRID_SMTP_START_TLS", default = true),
          ssl = envBoolean("SMART_GRID_SMTP_SSL", default = false),
          connectionTimeoutMs = envInt("SMART_GRID_SMTP_CONNECTION_TIMEOUT_MS", 10000),
          timeoutMs = envInt("SMART_GRID_SMTP_TIMEOUT_MS", 10000)
        )
      )
    )

  private def env(name: String): Option[String] =
    sys.env
      .find { case (key, _) => key == name }
      .map { case (_, value) => value.trim }
      .filter(_.nonEmpty)

  private def envList(name: String): List[String] =
    env(name)
      .map(_.split("[,;\\r\\n]+").toList.map(_.trim).filter(_.nonEmpty).distinct)
      .getOrElse(List.empty)

  private def envInt(name: String, default: Int): Int =
    env(name).flatMap(_.toIntOption).getOrElse(default)

  private def envLong(name: String, default: Long): Long =
    env(name).flatMap(_.toLongOption).getOrElse(default)

  private def envBoolean(name: String, default: Boolean): Boolean =
    env(name).flatMap(_.toBooleanOption).getOrElse(default)
}

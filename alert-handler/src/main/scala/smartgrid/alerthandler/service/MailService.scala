package smartgrid.alerthandler.service

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, StandardOpenOption}
import java.util.{Date, Properties}
import java.util.regex.Pattern

import cats.effect.IO
import cats.syntax.all._
import jakarta.mail.internet.{InternetAddress, MimeMessage}
import jakarta.mail.{Address, Authenticator, Message, PasswordAuthentication, Session, Transport}
import smartgrid.alerthandler.config.{MailConfig, MailMode, SmtpConfig}
import smartgrid.alerthandler.model.StoredAlert

final class MailService(
    config: MailConfig,
    path: Path,
    recipientStore: MailRecipientStore
) {
  import MailService.notificationSeparator

  def appendNotification(alert: StoredAlert): IO[Either[String, Unit]] =
    config.mode match {
      case MailMode.File =>
        append(render(alert, "FILE notification written") + notificationSeparator)

      case MailMode.Smtp =>
        sendSmtp(alert).flatMap {
          case Left(error) => IO.pure(Left(error))
          case Right(recipients) =>
            append(render(alert, s"SMTP email sent to ${recipients.mkString(", ")}") + notificationSeparator)
        }
    }

  def readRecipients: IO[Either[String, List[String]]] =
    recipientStore.readRecipients

  def saveRecipients(rawRecipients: String): IO[Either[String, List[String]]] =
    recipientStore.saveRecipients(rawRecipients)

  def recipientsPath: Path =
    recipientStore.path

  def readNotifications: IO[Either[String, List[String]]] =
    if (Files.exists(path)) {
      IO.blocking(Files.readString(path, StandardCharsets.UTF_8))
        .attempt
        .map(_.leftMap(error => s"Cannot read ${path.toString}: ${error.getMessage}"))
        .map(
          _.map(content =>
            content
              .split(Pattern.quote(notificationSeparator))
              .toList
              .map(_.trim)
              .filter(_.nonEmpty)
          )
        )
    } else {
      IO.pure(Right(List.empty))
    }

  def render(alert: StoredAlert): String =
    render(alert, "not sent yet")

  private def render(alert: StoredAlert, delivery: String): String =
    s"""Subject: ${subject(alert)}
       |Delivery: $delivery
       |
       |Alert id: ${alert.alertId}
       |Severity: ${alert.severity}
       |Reason: ${alert.reason}
       |Detected at: ${alert.detectedAt}
       |
       |Sensor: ${safe(alert.source.sensorId, "unknown-sensor")}
       |Transformer: ${safe(alert.source.transformerId, "unknown-transformer")}
       |Region: ${safe(alert.source.region, "unknown-region")}
       |Failure risk score: ${alert.source.failureRiskScore}
       |Temperature: ${alert.source.temperature}
       |Voltage: ${alert.source.voltage}
       |Current: ${alert.source.current}
       |Load: ${alert.source.load}
       |Status: ${safe(alert.source.status, "unknown-status")}
       |
       |Recommended action:
       |Inspect the impacted grid equipment.
       |""".stripMargin.trim

  private def subject(alert: StoredAlert): String =
    s"[CRITICAL] Smart Grid alert in ${safe(alert.source.region, "unknown-region")}"

  private def body(alert: StoredAlert): String =
    s"""Alert id: ${alert.alertId}
       |Severity: ${alert.severity}
       |Reason: ${alert.reason}
       |Detected at: ${alert.detectedAt}
       |
       |Sensor: ${safe(alert.source.sensorId, "unknown-sensor")}
       |Transformer: ${safe(alert.source.transformerId, "unknown-transformer")}
       |Region: ${safe(alert.source.region, "unknown-region")}
       |Failure risk score: ${alert.source.failureRiskScore}
       |Temperature: ${alert.source.temperature}
       |Voltage: ${alert.source.voltage}
       |Current: ${alert.source.current}
       |Load: ${alert.source.load}
       |Status: ${safe(alert.source.status, "unknown-status")}
       |
       |Recommended action:
       |Inspect the impacted grid equipment.
       |""".stripMargin.trim

  private def sendSmtp(alert: StoredAlert): IO[Either[String, List[String]]] =
    recipientStore.readRecipients.flatMap {
      case Left(error) => IO.pure(Left(error))
      case Right(recipients) =>
        validateSmtp(config, recipients) match {
          case Left(error) => IO.pure(Left(error))
          case Right(smtp) =>
            session(smtp) match {
              case Left(error) => IO.pure(Left(error))
              case Right(mailSession) =>
                IO.blocking {
                  val message = new MimeMessage(mailSession)
                  val recipientAddresses: Array[Address] =
                    recipients.map(recipient => new InternetAddress(recipient): Address).toArray

                  message.setFrom(new InternetAddress(config.from))
                  message.setRecipients(Message.RecipientType.TO, recipientAddresses)
                  message.setSubject(subject(alert), StandardCharsets.UTF_8.name())
                  message.setText(body(alert), StandardCharsets.UTF_8.name())
                  message.setSentDate(new Date())
                  Transport.send(message)
                  recipients
                }.attempt.map(_.leftMap(error => s"Cannot send SMTP email: ${error.getMessage}"))
            }
        }
    }

  private def session(smtp: SmtpConfig): Either[String, Session] =
    if (smtp.auth) {
      (smtp.username, smtp.password) match {
        case (Some(username), Some(password)) =>
          Right(
            Session.getInstance(
              smtpProperties(smtp),
              new Authenticator {
                override def getPasswordAuthentication: PasswordAuthentication =
                  new PasswordAuthentication(username, password)
              }
            )
          )

        case _ =>
          Left("SMTP authentication is enabled but username/password are missing.")
      }
    } else {
      Right(Session.getInstance(smtpProperties(smtp)))
    }

  private def smtpProperties(smtp: SmtpConfig): Properties = {
    val props = new Properties()
    List(
      "mail.transport.protocol" -> "smtp",
      "mail.smtp.host" -> smtp.host,
      "mail.smtp.port" -> smtp.port.toString,
      "mail.smtp.auth" -> smtp.auth.toString,
      "mail.smtp.starttls.enable" -> smtp.startTls.toString,
      "mail.smtp.ssl.enable" -> smtp.ssl.toString,
      "mail.smtp.connectiontimeout" -> smtp.connectionTimeoutMs.toString,
      "mail.smtp.timeout" -> smtp.timeoutMs.toString,
      "mail.smtp.writetimeout" -> smtp.timeoutMs.toString
    ).foreach { case (key, value) => props.put(key, value) }
    props
  }

  private def validateSmtp(config: MailConfig, recipients: List[String]): Either[String, SmtpConfig] = {
    val smtp = config.smtp
    val missing = List(
      required("mail.from", config.from),
      required("mail recipients", recipients.mkString(",")),
      required("mail.smtp.host", smtp.host)
    ).flatten ++ authMissingFields(smtp)

    if (missing.isEmpty) {
      Right(smtp)
    } else {
      Left(s"SMTP mail is not configured. Missing values: ${missing.mkString(", ")}")
    }
  }

  private def authMissingFields(smtp: SmtpConfig): List[String] =
    if (smtp.auth) {
      List(
        smtp.username.filter(_.nonEmpty).fold(Option("mail.smtp.username"))(_ => None),
        smtp.password.filter(_.nonEmpty).fold(Option("mail.smtp.password"))(_ => None)
      ).flatten
    } else {
      List.empty
    }

  private def required(name: String, value: String): Option[String] =
    Option(value).map(_.trim).filter(_.nonEmpty).fold(Option(name))(_ => None)

  private def append(content: String): IO[Either[String, Unit]] =
    ensureParent *>
      IO.blocking {
        Files.write(
          path,
          content.getBytes(StandardCharsets.UTF_8),
          StandardOpenOption.CREATE,
          StandardOpenOption.APPEND
        )
        ()
      }.attempt.map(_.leftMap(error => s"Cannot write ${path.toString}: ${error.getMessage}"))

  private def ensureParent: IO[Unit] =
    Option(path.getParent)
      .fold(IO.unit)(parent => IO.blocking(Files.createDirectories(parent)).void)

  private def safe(value: String, fallback: String): String =
    Option(value).map(_.trim).filter(_.nonEmpty).getOrElse(fallback)
}

object MailService {
  val notificationSeparator: String =
    System.lineSeparator() + "--- SMART-GRID-NOTIFICATION-END ---" + System.lineSeparator()
}

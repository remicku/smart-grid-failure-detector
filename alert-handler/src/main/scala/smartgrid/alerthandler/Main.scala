package smartgrid.alerthandler

import cats.effect.{ExitCode, IO, IOApp}
import cats.effect.std.Dispatcher
import cats.syntax.all._
import smartgrid.alerthandler.api.AlertApi
import smartgrid.alerthandler.config.AppConfig
import smartgrid.alerthandler.kafka.AlertConsumer
import smartgrid.alerthandler.model.{AlertSeverity, StoredAlert}
import smartgrid.alerthandler.service.{
  AlertBusinessLogic,
  MailRecipientStore,
  MailService,
  PostgresAlertRepository
}
import smartgrid.shared.{AlertMessage, SensorMessage}

object Main extends IOApp {
  override def run(args: List[String]): IO[ExitCode] = {
    val config = AppConfig.load
    val recipientStore = new MailRecipientStore(config.mail.recipientsPath, config.mail.recipients)
    val mailService = new MailService(config.mail, config.storage.notificationsPath, recipientStore)

    if (args.contains("--send-test-mail")) {
      sendTestMail(mailService).as(ExitCode.Success)
    } else {
      startService(config, mailService).as(ExitCode.Success)
    }
  }

  private def startService(config: AppConfig, mailService: MailService): IO[Unit] = {
    val repository = new PostgresAlertRepository(config.database)

    repository.init.flatMap(
      _.fold(
        error =>
          IO.delay(Console.err.println(s"[alert-handler] PostgreSQL initialization failed: $error")) *>
            IO.raiseError(new RuntimeException(error)),
        _ => IO.println("[alert-handler] PostgreSQL alert repository ready.")
      )
    ) *>
        Dispatcher.parallel[IO].use { dispatcher =>
          val businessLogic = new AlertBusinessLogic(repository, mailService)
          val api =
            new AlertApi(config.server, config.kafka.alertsTopic, repository, mailService, businessLogic, dispatcher)
          val consumer =
            AlertConsumer
              .stream(config.kafka, businessLogic)
              .compile
              .drain
              .handleErrorWith(error =>
                IO.delay(
                  Console.err.println(
                    s"[alert-handler] Kafka consumer stopped: ${error.getMessage}. HTTP API stays available."
                  )
                ) *> IO.never
              )

          api.start *> consumer.guarantee(api.stop)
        }
  }

  private def sendTestMail(mailService: MailService): IO[Unit] =
    mailService.appendNotification(testAlert).flatMap(
      _.fold(
        error => IO.delay(Console.err.println(s"[alert-handler] Test email failed: $error")),
        _ => IO.println("[alert-handler] Test email sent successfully.")
      )
    )

  private def testAlert: StoredAlert =
    StoredAlert.from(
      AlertMessage(
        alertId = "test-alert-email",
        severity = "CRITICAL",
        reason = "Manual SMTP test from alert-handler",
        detectedAt = System.currentTimeMillis(),
        source = SensorMessage(
          sensorId = "test-sensor",
          transformerId = "test-transformer",
          timestamp = java.time.Instant.now().toString,
          region = "Test-Region",
          voltage = 220.0,
          current = 30.0,
          temperature = 75.0,
          load = 78.0,
          failureRiskScore = 0.95,
          status = "ONLINE"
        )
      ),
      AlertSeverity.Critical
    )
}

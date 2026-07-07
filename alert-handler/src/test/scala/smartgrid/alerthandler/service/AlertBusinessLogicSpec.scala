package smartgrid.alerthandler.service

import cats.effect.unsafe.implicits.global
import smartgrid.alerthandler.config.{MailConfig, MailMode, SmtpConfig}
import smartgrid.alerthandler.model.{AlertSeverity, StoredAlert}
import smartgrid.shared.{AlertMessage, SensorMessage}

class AlertBusinessLogicSpec extends munit.FunSuite {
  test("actions generated when severity is WARNING") {
    val actions = AlertBusinessLogic.actionsFor(sampleAlert("WARNING"))

    assertEquals(
      actionNames(actions),
      List("LogAlert", "StoreAlert", "AddToDashboardState")
    )
  }

  test("actions generated when severity is CRITICAL") {
    val actions = AlertBusinessLogic.actionsFor(sampleAlert("CRITICAL"))

    assertEquals(
      actionNames(actions),
      List(
        "LogAlert",
        "StoreAlert",
        "StoreCriticalAlert",
        "SendMailNotification",
        "AddToDashboardState"
      )
    )
  }

  test("actions generated when severity is unknown") {
    val actions = AlertBusinessLogic.actionsFor(sampleAlert("INFO"))

    assertEquals(
      actionNames(actions),
      List("LogAlert", "StoreAlert", "AddToDashboardState")
    )
  }

  test("mail rendering contains shared model fields") {
    val stored = StoredAlert.from(sampleAlert("CRITICAL"), AlertSeverity.Critical)
    val rendered =
      new MailService(
        testMailConfig,
        java.nio.file.Path.of("target/test-emails.log"),
        testRecipientStore("target/test-recipients-render.txt", testMailConfig)
      ).render(stored)

    assert(rendered.contains("Subject: [CRITICAL] Smart Grid alert in Paris-13"))
    assert(rendered.contains("Transformer: transformer-42"))
    assert(rendered.contains("Failure risk score: 0.91"))
  }

  test("smtp mail mode reports missing configuration cleanly") {
    val stored = StoredAlert.from(sampleAlert("CRITICAL"), AlertSeverity.Critical)
    val result =
      new MailService(
        incompleteSmtpConfig,
        java.nio.file.Path.of("target/test-emails.log"),
        testRecipientStore("target/test-recipients-incomplete.txt", incompleteSmtpConfig)
      ).appendNotification(stored).unsafeRunSync()

    assert(result.left.toOption.exists(_.contains("SMTP mail is not configured")))
  }

  test("mail recipient store saves one address per line") {
    val store = testRecipientStore("target/test-recipients-store.txt", testMailConfig)
    val result = store.saveRecipients("first@example.com\nsecond@example.com, first@example.com").unsafeRunSync()

    assertEquals(result, Right(List("first@example.com", "second@example.com")))
    assertEquals(store.readRecipients.unsafeRunSync(), Right(List("first@example.com", "second@example.com")))
  }

  test("dashboard state filters alerts by region") {
    val parisAlert = StoredAlert.from(sampleAlert("WARNING"), AlertSeverity.Warning)
    val lyonAlert = parisAlert.copy(alertId = "alert-002", source = parisAlert.source.copy(region = "Lyon-07"))
    val state = AlertState.from(Vector(parisAlert, lyonAlert)).unsafeRunSync()

    assertEquals(state.byRegion("paris-13").unsafeRunSync().map(_.alertId), Vector("alert-001"))
  }

  private def sampleAlert(severity: String): AlertMessage =
    AlertMessage(
      alertId = "alert-001",
      severity = severity,
      reason = "failureRiskScore=0.91 -> CRITICAL",
      detectedAt = 1782412800000L,
      source = SensorMessage(
        sensorId = "sensor-42",
        transformerId = "transformer-42",
        timestamp = 1782412799000L,
        region = "Paris-13",
        voltage = 207.3,
        current = 31.2,
        temperature = 81.2,
        load = 86.0,
        failureRiskScore = 0.91,
        status = "ONLINE"
      )
    )

  private def actionNames(actions: List[AlertAction]): List[String] =
    actions.map {
      case LogAlert(_, _)          => "LogAlert"
      case StoreAlert(_)           => "StoreAlert"
      case StoreCriticalAlert(_)   => "StoreCriticalAlert"
      case SendMailNotification(_) => "SendMailNotification"
      case AddToDashboardState(_)  => "AddToDashboardState"
    }

  private val testMailConfig: MailConfig =
    MailConfig(
      mode = MailMode.File,
      from = "alerts@example.com",
      recipients = List("operator@example.com"),
      recipientsPath = java.nio.file.Path.of("target/test-recipients-default.txt"),
      smtp = SmtpConfig(
        host = "smtp.example.com",
        port = 587,
        username = Some("user"),
        password = Some("password"),
        auth = true,
        startTls = true,
        ssl = false,
        connectionTimeoutMs = 10000,
        timeoutMs = 10000
      )
    )

  private val incompleteSmtpConfig: MailConfig =
    testMailConfig.copy(
      mode = MailMode.Smtp,
      from = "",
      recipients = List.empty,
      smtp = testMailConfig.smtp.copy(host = "", username = None, password = None)
    )

  private def testRecipientStore(path: String, config: MailConfig): MailRecipientStore =
    new MailRecipientStore(java.nio.file.Path.of(path), config.recipients)
}

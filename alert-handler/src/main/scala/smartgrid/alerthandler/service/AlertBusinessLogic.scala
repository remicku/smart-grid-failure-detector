package smartgrid.alerthandler.service

import cats.effect.IO
import cats.syntax.all._
import smartgrid.alerthandler.model.AlertSeverity.{Critical, UnknownSeverity, Warning}
import smartgrid.alerthandler.model.{AlertSeverity, StoredAlert}
import smartgrid.shared.AlertMessage

final class AlertBusinessLogic(
    repository: AlertRepository,
    mailService: MailService
) {
  def handle(message: AlertMessage): IO[List[ActionResult]] =
    AlertBusinessLogic
      .actionsFor(message)
      .traverse(runAction)
      .flatTap(results =>
        results.collect { case ActionFailed(actionName, error) =>
          IO.delay(Console.err.println(s"[alert-handler] Action failed: $actionName - $error"))
        }.sequence_
      )

  private def runAction(action: AlertAction): IO[ActionResult] =
    action match {
      case LogAlert(alert, severity) =>
        ConsoleAlertLogger.log(alert, severity).as(ActionSucceeded("log-alert"))

      case StoreAlert(alert) =>
        repository.appendAlert(alert).flatMap(
          _.fold(
            error => IO.raiseError(new RuntimeException(error)),
            _ => IO.pure(ActionSucceeded("store-alert"))
          )
        )

      case StoreCriticalAlert(alert) =>
        repository.appendCriticalAlert(alert).map(
          _.fold(
            error => ActionFailed("store-critical-alert", error),
            _ => ActionSucceeded("store-critical-alert")
          )
        )

      case SendMailNotification(alert) =>
        sendMailNotification(alert)

      case AddToDashboardState(_) =>
        IO.pure(ActionSucceeded("dashboard-reads-postgres"))
    }

  private def sendMailNotification(alert: StoredAlert): IO[ActionResult] =
    repository.claimMailNotification(alert.alertId).flatMap(
      _.fold(
        error => IO.pure(ActionFailed("claim-mail-notification", error)),
        claimed =>
          if (claimed) {
            mailService.appendNotification(alert).flatMap(result => recordMailResult(alert, result))
          } else {
            IO.pure(ActionSucceeded("send-mail-notification-skipped"))
          }
      )
    )

  private def recordMailResult(alert: StoredAlert, result: Either[String, Unit]): IO[ActionResult] =
    result match {
      case Left(error) =>
        repository
          .recordMailNotification(alert.alertId, "FAILED", Some(error))
          .map(
            _.fold(
              statusError => ActionFailed("record-mail-notification", statusError),
              _ => ActionFailed("send-mail-notification", error)
            )
          )

      case Right(_) =>
        repository
          .recordMailNotification(alert.alertId, "SENT", None)
          .map(
            _.fold(
              statusError => ActionFailed("record-mail-notification", statusError),
              _ => ActionSucceeded("send-mail-notification")
            )
          )
    }
}

object AlertBusinessLogic {
  def actionsFor(message: AlertMessage): List[AlertAction] = {
    val severity = AlertSeverity.parse(message.severity)
    val stored = StoredAlert.from(message, severity)

    severity match {
      case Warning =>
        List(
          LogAlert(stored, severity),
          StoreAlert(stored),
          AddToDashboardState(stored)
        )

      case Critical =>
        List(
          LogAlert(stored, severity),
          StoreAlert(stored),
          StoreCriticalAlert(stored),
          SendMailNotification(stored),
          AddToDashboardState(stored)
        )

      case UnknownSeverity(_) =>
        List(
          LogAlert(stored, severity),
          StoreAlert(stored),
          AddToDashboardState(stored)
        )
    }
  }
}

package smartgrid.alerthandler.service

import cats.effect.IO
import cats.syntax.all._
import smartgrid.alerthandler.model.AlertSeverity.{Critical, UnknownSeverity, Warning}
import smartgrid.alerthandler.model.{AlertSeverity, StoredAlert}
import smartgrid.shared.AlertMessage

final class AlertBusinessLogic(
    storage: AlertStorage,
    mailService: MailService,
    state: AlertState
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
        storage.appendAlert(alert).map(
          _.fold(
            error => ActionFailed("store-alert", error),
            _ => ActionSucceeded("store-alert")
          )
        )

      case StoreCriticalAlert(alert) =>
        storage.appendCriticalAlert(alert).map(
          _.fold(
            error => ActionFailed("store-critical-alert", error),
            _ => ActionSucceeded("store-critical-alert")
          )
        )

      case SendMailNotification(alert) =>
        mailService.appendNotification(alert).map(
          _.fold(
            error => ActionFailed("send-mail-notification", error),
            _ => ActionSucceeded("send-mail-notification")
          )
        )

      case AddToDashboardState(alert) =>
        state.add(alert).as(ActionSucceeded("add-to-dashboard-state"))
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

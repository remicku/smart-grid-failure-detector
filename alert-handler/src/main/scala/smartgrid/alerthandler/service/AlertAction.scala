package smartgrid.alerthandler.service

import smartgrid.alerthandler.model.{AlertSeverity, StoredAlert}

sealed trait AlertAction {
  def alert: StoredAlert
}

final case class StoreAlert(alert: StoredAlert) extends AlertAction
final case class StoreCriticalAlert(alert: StoredAlert) extends AlertAction
final case class SendMailNotification(alert: StoredAlert) extends AlertAction
final case class LogAlert(alert: StoredAlert, severity: AlertSeverity) extends AlertAction
final case class AddToDashboardState(alert: StoredAlert) extends AlertAction

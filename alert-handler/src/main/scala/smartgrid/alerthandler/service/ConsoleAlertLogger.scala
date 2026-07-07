package smartgrid.alerthandler.service

import cats.effect.IO
import smartgrid.alerthandler.model.AlertSeverity.{Critical, UnknownSeverity, Warning}
import smartgrid.alerthandler.model.{AlertSeverity, StoredAlert}

object ConsoleAlertLogger {
  def log(alert: StoredAlert, severity: AlertSeverity): IO[Unit] =
    severity match {
      case Warning =>
        IO.println(
          s"[WARNING] alert=${alert.alertId} transformer=${alert.source.transformerId} region=${safe(alert.source.region)} reason=${alert.reason}"
        )

      case Critical =>
        IO.println(
          s"""
             |============================================================
             |[CRITICAL] SMART GRID ALERT
             |Alert id    : ${alert.alertId}
             |Transformer : ${alert.source.transformerId}
             |Region      : ${safe(alert.source.region)}
             |Reason      : ${alert.reason}
             |Risk score  : ${alert.source.failureRiskScore}
             |Temperature : ${alert.source.temperature}
             |Voltage     : ${alert.source.voltage}
             |Load        : ${alert.source.load}
             |============================================================
             |""".stripMargin.trim
        )

      case UnknownSeverity(value) =>
        IO.delay(
          Console.err.println(
            s"[UNKNOWN SEVERITY] alert=${alert.alertId} severity=$value region=${safe(alert.source.region)} reason=${alert.reason}"
          )
        )
    }

  private def safe(value: String): String =
    Option(value).map(_.trim).filter(_.nonEmpty).getOrElse("unknown-region")
}

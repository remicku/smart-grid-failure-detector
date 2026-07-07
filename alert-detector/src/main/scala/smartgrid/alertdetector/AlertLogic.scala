package smartgrid.alertdetector

import smartgrid.shared.SensorMessage

object AlertLogic {

  val WarningThreshold: Double  = 0.8
  val CriticalThreshold: Double = 0.9

  def severityFor(score: Double): Option[String] =
    if (score >= CriticalThreshold) Some("CRITICAL")
    else if (score >= WarningThreshold) Some("WARNING")
    else None

  def reasonFor(msg: SensorMessage, severity: String): String =
    s"failureRiskScore=${msg.failureRiskScore} -> $severity " +
      s"(warn>=$WarningThreshold, crit>=$CriticalThreshold)"
}

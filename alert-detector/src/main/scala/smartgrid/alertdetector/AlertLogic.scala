package smartgrid.alertdetector

import smartgrid.shared.{AlertThresholds, SensorMessage}

object AlertLogic {

  val WarningThreshold: Double  = AlertThresholds.WarningThreshold
  val CriticalThreshold: Double = AlertThresholds.CriticalThreshold

  def severityFor(score: Double): Option[String] =
    if (score >= CriticalThreshold) Some("CRITICAL")
    else if (score >= WarningThreshold) Some("WARNING")
    else None

  def reasonFor(msg: SensorMessage, severity: String): String =
    s"failureRiskScore=${msg.failureRiskScore} -> $severity " +
      s"(warn>=$WarningThreshold, crit>=$CriticalThreshold)"
}

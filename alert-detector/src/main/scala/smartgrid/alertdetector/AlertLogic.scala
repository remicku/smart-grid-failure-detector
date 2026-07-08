package smartgrid.alertdetector

import smartgrid.shared.{AlertThresholds, SensorMessage}

object AlertLogic {

  val WarningThreshold: Double  = AlertThresholds.WarningThreshold
  val CriticalThreshold: Double = AlertThresholds.CriticalThreshold

  def severityFor(score: Double): Option[String] =
    if (score >= CriticalThreshold) Some("CRITICAL")
    else if (score >= WarningThreshold) Some("WARNING")
    else None

  def evaluate(since: Option[Long], score: Double, now: Long, window: Long): (Option[Long], Option[String]) = {
    val next      = if (score >= WarningThreshold) since.orElse(Some(now)) else None
    val sustained = next.exists(start => now - start >= window)
    val severity  = if (score >= CriticalThreshold || sustained) severityFor(score) else None
    (next, severity)
  }

  def reasonFor(msg: SensorMessage, severity: String): String =
    s"failureRiskScore=${msg.failureRiskScore} -> $severity " +
      s"(warn>=$WarningThreshold, crit>=$CriticalThreshold)"
}

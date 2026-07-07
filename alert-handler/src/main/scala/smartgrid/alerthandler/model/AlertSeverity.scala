package smartgrid.alerthandler.model

import java.util.Locale

sealed trait AlertSeverity {
  def category: String
  def displayValue: String
}

object AlertSeverity {
  case object Warning extends AlertSeverity {
    override val category: String = "WARNING"
    override val displayValue: String = "WARNING"
  }

  case object Critical extends AlertSeverity {
    override val category: String = "CRITICAL"
    override val displayValue: String = "CRITICAL"
  }

  final case class UnknownSeverity(value: String) extends AlertSeverity {
    override val category: String = "UNKNOWN"
    override val displayValue: String = s"UNKNOWN($value)"
  }

  def parse(value: String): AlertSeverity =
    Option(value)
      .map(_.trim)
      .filter(_.nonEmpty)
      .map(_.toUpperCase(Locale.ROOT))
      .fold[AlertSeverity](UnknownSeverity("UNKNOWN")) {
        case "WARNING"  => Warning
        case "CRITICAL" => Critical
        case other      => UnknownSeverity(other)
      }
}

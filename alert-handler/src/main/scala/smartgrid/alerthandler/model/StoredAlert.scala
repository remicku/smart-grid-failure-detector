package smartgrid.alerthandler.model

import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import smartgrid.shared.{AlertMessage, SensorMessage}

final case class StoredAlert(
    alertId: String,
    severity: String,
    originalSeverity: String,
    reason: String,
    detectedAt: Long,
    source: SensorMessage
)

object StoredAlert {
  implicit val decoder: Decoder[StoredAlert] = deriveDecoder
  implicit val encoder: Encoder[StoredAlert] = deriveEncoder

  def from(message: AlertMessage, severity: AlertSeverity): StoredAlert =
    StoredAlert(
      alertId = clean(message.alertId, "missing-alert-id"),
      severity = severity.category,
      originalSeverity = clean(message.severity, "UNKNOWN"),
      reason = clean(message.reason, "No reason provided"),
      detectedAt = message.detectedAt,
      source = message.source
    )

  private def clean(value: String, fallback: String): String =
    Option(value).map(_.trim).filter(_.nonEmpty).getOrElse(fallback)
}

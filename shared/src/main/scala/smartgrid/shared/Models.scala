package smartgrid.shared

import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}

final case class SensorMessage(
    sensorId: String,
    transformerId: String,
    timestamp: Long,
    region: String,
    voltage: Double,
    current: Double,
    temperature: Double,
    load: Double,
    failureRiskScore: Double,
    status: String
)

object SensorMessage {
  implicit val decoder: Decoder[SensorMessage] = deriveDecoder
  implicit val encoder: Encoder[SensorMessage] = deriveEncoder
}

final case class AlertMessage(
    alertId: String,
    severity: String, // "WARNING" | "CRITICAL"
    reason: String,
    detectedAt: Long,
    source: SensorMessage
)

object AlertMessage {
  implicit val decoder: Decoder[AlertMessage] = deriveDecoder
  implicit val encoder: Encoder[AlertMessage] = deriveEncoder
}

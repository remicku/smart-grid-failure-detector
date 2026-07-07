import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.time.Instant
import java.util.Properties
import scala.util.Random

final case class SensorReading(
    sensorId: String,
    transformerId: String,
    timestamp: String,
    region: String,
    voltage: Double,
    current: Double,
    temperature: Double,
    load: Double,
    failureRiskScore: Double,
    status: String
)

object SensorSimulator:

  val regions: List[String] =
    List("Ile-de-France", "Auvergne-Rhone-Alpes", "Occitanie", "Bretagne", "Grand-Est")

  // Borne la valeur entre 0 et 1
  def clamp01(value: Double): Double = math.max(0.0, math.min(1.0, value))

  // 2 chiffres après la virgule
  def round2(value: Double): Double = math.round(value * 100.0) / 100.0

  
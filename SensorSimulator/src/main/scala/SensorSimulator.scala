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

  def riskScore(voltage: Double, temperature: Double, load: Double): Double =
    val temperatureRisk = clamp01((temperature - 60.0) / 40.0)
    val loadRisk = clamp01((load - 70.0) / 30.0)
    val voltageRisk = clamp01(math.abs(voltage - 230.0) / 30.0)
    clamp01(0.5 * temperatureRisk + 0.3 * loadRisk + 0.2 * voltageRisk)

  def statusOf(score: Double): String =
    if score >= 0.7 then "CRITICAL"
    else if score >= 0.4 then "WARNING"
    else "NORMAL"

  def nextReading(random: Random): SensorReading =
    val voltage = round2(215.0 + random.nextDouble() * 30.0)
    val load = round2(random.nextDouble() * 100.0)
    val current = round2(5.0 + load * 0.9 + random.nextGaussian())
    val temperature = round2(30.0 + load * 0.5 + random.nextGaussian() * 3.0)
    val score = round2(riskScore(voltage, temperature, load))
    SensorReading(
      sensorId = s"S-${random.nextInt(100000)}",
      transformerId = s"TR-${random.nextInt(1000)}",
      timestamp = Instant.now().toString,
      region = regions(random.nextInt(regions.size)),
      voltage = voltage,
      current = current,
      temperature = temperature,
      load = load,
      failureRiskScore = score,
      status = statusOf(score)
    )

  def toJson(r: SensorReading): String =
    s"""{"sensorId":"${r.sensorId}","transformerId":"${r.transformerId}",""" +
      s""""timestamp":"${r.timestamp}","region":"${r.region}",""" +
      s""""voltage":${r.voltage},"current":${r.current},""" +
      s""""temperature":${r.temperature},"load":${r.load},""" +
      s""""failureRiskScore":${r.failureRiskScore},"status":"${r.status}"}"""

 
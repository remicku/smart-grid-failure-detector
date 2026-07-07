package smartgrid.simulator

import cats.effect.IO
import cats.effect.std.Random
import cats.syntax.all._
import smartgrid.shared.{AlertThresholds, SensorMessage}

object SensorSimulator {

  val regions: List[String] =
    List("Ile-de-France", "Auvergne-Rhone-Alpes", "Occitanie", "Bretagne", "Grand-Est")

  def clamp01(value: Double): Double = math.max(0.0, math.min(1.0, value))

  def round2(value: Double): Double = math.round(value * 100.0) / 100.0

  def riskScore(voltage: Double, temperature: Double, load: Double): Double = {
    val temperatureRisk = clamp01((temperature - 60.0) / 40.0)
    val loadRisk        = clamp01((load - 70.0) / 30.0)
    val voltageRisk     = clamp01(math.abs(voltage - 230.0) / 30.0)
    clamp01(0.5 * temperatureRisk + 0.3 * loadRisk + 0.2 * voltageRisk)
  }

  def statusOf(score: Double): String =
    if (score >= AlertThresholds.CriticalThreshold) "CRITICAL"
    else if (score >= AlertThresholds.WarningThreshold) "WARNING"
    else "NORMAL"

  def nextMessage(random: Random[IO]): IO[SensorMessage] =
    (
      random.nextDouble,
      random.nextDouble,
      random.nextGaussian,
      random.nextGaussian,
      random.betweenInt(0, 100000),
      random.betweenInt(0, 1000),
      random.betweenInt(0, regions.size),
      IO.realTimeInstant
    ).mapN { (voltageRaw, loadRaw, currentNoise, temperatureNoise, sensorNum, transformerNum, regionIdx, now) =>
      val voltage     = round2(215.0 + voltageRaw * 30.0)
      val load        = round2(loadRaw * 100.0)
      val current     = round2(5.0 + load * 0.9 + currentNoise)
      val temperature = round2(30.0 + load * 0.5 + temperatureNoise * 3.0)
      val score       = round2(riskScore(voltage, temperature, load))
      SensorMessage(
        sensorId = s"S-$sensorNum",
        transformerId = s"TR-$transformerNum",
        timestamp = now.toString,
        region = regions(regionIdx),
        voltage = voltage,
        current = current,
        temperature = temperature,
        load = load,
        failureRiskScore = score,
        status = statusOf(score)
      )
    }
}

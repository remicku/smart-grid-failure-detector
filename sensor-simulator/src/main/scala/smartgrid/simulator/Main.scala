package smartgrid.simulator

import scala.concurrent.duration._

import cats.effect.{IO, IOApp}
import cats.effect.std.Random
import fs2.Stream
import fs2.kafka._
import io.circe.syntax._
import smartgrid.shared.SensorMessage

object Main extends IOApp.Simple {

  private val Bootstrap   = "localhost:9092,localhost:9094,localhost:9096"
  private val OutputTopic = "ST"
  private val Interval    = 1.second

  private val producerSettings =
    ProducerSettings[IO, String, String]
      .withBootstrapServers(Bootstrap)

  private def record(msg: SensorMessage): ProducerRecords[String, String] =
    ProducerRecords.one(
      ProducerRecord(OutputTopic, msg.transformerId, msg.asJson.noSpaces)
    )

  val run: IO[Unit] =
    Random.scalaUtilRandom[IO].flatMap { random =>
      Stream
        .fixedRate[IO](Interval)
        .evalMap(_ => SensorSimulator.nextMessage(random))
        .map(record)
        .through(KafkaProducer.pipe(producerSettings))
        .compile
        .drain
    }
}

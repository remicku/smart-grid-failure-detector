package smartgrid.alertdetector

import scala.concurrent.duration._

import cats.effect.{IO, IOApp}
import cats.effect.std.{Console, UUIDGen}
import cats.syntax.all._
import fs2.kafka._
import io.circe.parser.decode
import io.circe.syntax._
import smartgrid.shared.{AlertMessage, SensorMessage}

object Main extends IOApp.Simple {

  private val Bootstrap   = "localhost:9092"
  private val InputTopic  = "ST"
  private val OutputTopic = "ST2"
  private val GroupId     = "alert-detector"

  private val consumerSettings =
    ConsumerSettings[IO, String, String]
      .withBootstrapServers(Bootstrap)
      .withGroupId(GroupId)
      .withAutoOffsetReset(AutoOffsetReset.Earliest)

  private val producerSettings =
    ProducerSettings[IO, String, String]
      .withBootstrapServers(Bootstrap)

  private def buildAlert(msg: SensorMessage): IO[Option[AlertMessage]] =
    AlertLogic.severityFor(msg.failureRiskScore) match {
      case None => IO.pure(None)
      case Some(severity) =>
        (UUIDGen[IO].randomUUID, IO.realTimeInstant).mapN { (id, now) =>
          AlertMessage(
            alertId = id.toString,
            severity = severity,
            reason = AlertLogic.reasonFor(msg, severity),
            detectedAt = now.toEpochMilli,
            source = msg
          ).some
        }
    }

  private def process(raw: String): IO[Option[AlertMessage]] =
    decode[SensorMessage](raw) match {
      case Left(err)     => Console[IO].errorln(s"[skip] message invalide: ${err.getMessage}").as(None)
      case Right(sensor) => buildAlert(sensor)
    }

  val run: IO[Unit] =
    KafkaConsumer
      .stream(consumerSettings)
      .subscribeTo(InputTopic)
      .records
      .mapAsync(16) { committable =>
        process(committable.record.value).map { maybeAlert =>
          maybeAlert.fold(
            ProducerRecords(List.empty[ProducerRecord[String, String]], committable.offset)
          ) { alert =>
            ProducerRecords.one(
              ProducerRecord(OutputTopic, alert.source.transformerId, alert.asJson.noSpaces),
              committable.offset
            )
          }
        }
      }
      .through(KafkaProducer.pipe(producerSettings))
      .map(_.passthrough)
      .through(commitBatchWithin(500, 15.seconds))
      .compile
      .drain
}

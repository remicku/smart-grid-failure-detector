package smartgrid.alertdetector

import scala.concurrent.duration._

import cats.effect.{IO, IOApp, Ref}
import cats.effect.std.{Console, UUIDGen}
import cats.syntax.all._
import fs2.Chunk
import fs2.kafka._
import io.circe.parser.decode
import io.circe.syntax._
import smartgrid.shared.{AlertMessage, SensorMessage}

object Main extends IOApp.Simple {

  private val Bootstrap   = "localhost:9092,localhost:9094,localhost:9096"
  private val InputTopic  = "ST"
  private val OutputTopic = "ST2"
  private val GroupId     = "alert-detector"
  private val Window      = 30.seconds

  private val consumerSettings =
    ConsumerSettings[IO, Array[Byte], String]
      .withBootstrapServers(Bootstrap)
      .withGroupId(GroupId)
      .withAutoOffsetReset(AutoOffsetReset.Earliest)

  private val producerSettings =
    ProducerSettings[IO, String, String]
      .withBootstrapServers(Bootstrap)

  private def buildAlert(state: Ref[IO, Map[String, Long]], msg: SensorMessage): IO[Option[AlertMessage]] =
    IO.realTimeInstant.flatMap { now =>
      state
        .modify { current =>
          val (since, severity) =
            AlertLogic.evaluate(current.get(msg.transformerId), msg.failureRiskScore, now.toEpochMilli, Window.toMillis)
          val updated = since.fold(current - msg.transformerId)(start => current.updated(msg.transformerId, start))
          (updated, severity)
        }
        .flatMap {
          case None => IO.pure(None)
          case Some(severity) =>
            UUIDGen[IO].randomUUID.map { id =>
              AlertMessage(
                alertId = id.toString,
                severity = severity,
                reason = AlertLogic.reasonFor(msg, severity),
                detectedAt = now.toEpochMilli,
                source = msg
              ).some
            }
        }
    }

  private def process(state: Ref[IO, Map[String, Long]], raw: String): IO[Option[AlertMessage]] =
    decode[SensorMessage](raw) match {
      case Left(err)     => Console[IO].errorln(s"[skip] message invalide: ${err.getMessage}").as(None)
      case Right(sensor) => buildAlert(state, sensor)
    }

  val run: IO[Unit] =
    Ref.of[IO, Map[String, Long]](Map.empty).flatMap { state =>
      KafkaProducer
        .stream(producerSettings)
        .flatMap { producer =>
          KafkaConsumer
            .stream(consumerSettings)
            .subscribeTo(InputTopic)
            .records
            .evalMap { committable =>
              process(state, committable.record.value).map { maybeAlert =>
                val records: ProducerRecords[String, String] =
                  maybeAlert.fold(Chunk.empty[ProducerRecord[String, String]]) { alert =>
                    Chunk.singleton(
                      ProducerRecord(OutputTopic, alert.source.transformerId, alert.asJson.noSpaces)
                    )
                  }
                committable.offset -> records
              }
            }
            .evalMap { case (offset, records) =>
              producer.produce(records).flatten.as(offset)
            }
            .through(commitBatchWithin(500, 15.seconds))
        }
        .compile
        .drain
    }
}

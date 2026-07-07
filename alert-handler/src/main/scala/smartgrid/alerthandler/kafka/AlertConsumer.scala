package smartgrid.alerthandler.kafka

import scala.concurrent.duration._

import cats.effect.IO
import cats.effect.std.Console
import cats.syntax.all._
import fs2.Stream
import fs2.kafka._
import io.circe.parser.decode
import smartgrid.alerthandler.config.KafkaConfig
import smartgrid.alerthandler.service.AlertBusinessLogic
import smartgrid.shared.AlertMessage

object AlertConsumer {
  def stream(config: KafkaConfig, businessLogic: AlertBusinessLogic): Stream[IO, Unit] = {
    val consumerSettings =
      ConsumerSettings[IO, Array[Byte], String]
        .withBootstrapServers(config.bootstrapServers)
        .withGroupId(config.groupId)
        .withAutoOffsetReset(AutoOffsetReset.Earliest)

    KafkaConsumer
      .stream(consumerSettings)
      .subscribeTo(config.alertsTopic)
      .records
      .evalMap(record =>
        process(record.record.value, businessLogic).as(record.offset)
      )
      .through(commitBatchWithin(500, config.pollTimeoutMs.millis))
  }

  private def process(raw: String, businessLogic: AlertBusinessLogic): IO[Unit] =
    decode[AlertMessage](raw) match {
      case Left(error) =>
        Console[IO].errorln(s"[alert-handler] Ignoring invalid alert JSON: ${error.getMessage}")

      case Right(alert) =>
        businessLogic.handle(alert).void
    }
}

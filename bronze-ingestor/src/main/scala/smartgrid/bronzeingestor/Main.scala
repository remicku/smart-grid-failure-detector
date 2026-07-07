package smartgrid.bronzeingestor

import cats.effect.{IO, IOApp}
import cats.effect.std.Console
import fs2.Stream
import fs2.kafka._
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, StandardOpenOption}

object Main extends IOApp.Simple {

  private val bootstrap = env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
  private val topic = env("TELEMETRY_TOPIC", "ST")
  private val groupId = env("BRONZE_GROUP_ID", "bronze-ingestor")
  private val output = Path.of(env("BRONZE_OUTPUT", "../data/bronze/telemetry.jsonl"))
  private val maxMessages = sys.env.lift("BRONZE_MAX_MESSAGES").flatMap(_.toLongOption)

  private val settings =
    ConsumerSettings[IO, String, String]
      .withBootstrapServers(bootstrap)
      .withGroupId(groupId)
      .withAutoOffsetReset(AutoOffsetReset.Earliest)

  private def env(name: String, fallback: String): String =
    sys.env.lift(name).filter(_.nonEmpty).getOrElse(fallback)

  private def writeBronzeLine(raw: String): IO[Unit] =
    IO.blocking {
      Files.createDirectories(output.getParent)
      Files.writeString(
        output,
        raw.replace("\n", "").replace("\r", "") + System.lineSeparator(),
        StandardCharsets.UTF_8,
        StandardOpenOption.CREATE,
        StandardOpenOption.APPEND
      )
    }.void

  private def limit[A](stream: Stream[IO, A]): Stream[IO, A] =
    maxMessages.fold(stream)(count => stream.take(count))

  val run: IO[Unit] =
    limit(
      KafkaConsumer
        .stream(settings)
        .subscribeTo(topic)
        .records
    )
      .evalMap { record =>
        writeBronzeLine(record.record.value) >>
          record.offset.commit >>
          Console[IO].println(s"[bronze] wrote one message to $output")
      }
      .compile
      .drain
}

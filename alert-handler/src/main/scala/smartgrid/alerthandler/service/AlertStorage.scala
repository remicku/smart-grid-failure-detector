package smartgrid.alerthandler.service

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, StandardOpenOption}

import cats.effect.IO
import cats.syntax.all._
import io.circe.parser.decode
import io.circe.syntax._
import smartgrid.alerthandler.config.StorageConfig
import smartgrid.alerthandler.model.StoredAlert

import scala.jdk.CollectionConverters._

final class AlertStorage(config: StorageConfig) {
  def appendAlert(alert: StoredAlert): IO[Either[String, Unit]] =
    appendJsonLine(config.alertsPath, alert)

  def appendCriticalAlert(alert: StoredAlert): IO[Either[String, Unit]] =
    appendJsonLine(config.criticalAlertsPath, alert)

  def loadAlerts: IO[Either[String, Vector[StoredAlert]]] =
    if (Files.exists(config.alertsPath)) {
      IO.blocking(Files.readAllLines(config.alertsPath, StandardCharsets.UTF_8).asScala.toVector)
        .attempt
        .map(_.leftMap(error => s"Cannot read ${config.alertsPath.toString}: ${error.getMessage}"))
        .map(_.flatMap(parseAlerts))
    } else {
      IO.pure(Right(Vector.empty))
    }

  private def appendJsonLine(path: Path, alert: StoredAlert): IO[Either[String, Unit]] =
    appendLine(path, alert.asJson.noSpaces)

  private def appendLine(path: Path, line: String): IO[Either[String, Unit]] =
    ensureParent(path) *>
      IO.blocking {
        Files.write(
          path,
          (line + System.lineSeparator()).getBytes(StandardCharsets.UTF_8),
          StandardOpenOption.CREATE,
          StandardOpenOption.APPEND
        )
        ()
      }.attempt.map(_.leftMap(error => s"Cannot write ${path.toString}: ${error.getMessage}"))

  private def ensureParent(path: Path): IO[Unit] =
    Option(path.getParent)
      .fold(IO.unit)(parent => IO.blocking(Files.createDirectories(parent)).void)

  private def parseAlerts(lines: Vector[String]): Either[String, Vector[StoredAlert]] =
    lines
      .map(_.trim)
      .filter(_.nonEmpty)
      .map(line => decode[StoredAlert](line).leftMap(error => s"Invalid stored alert JSON: ${error.getMessage}"))
      .foldLeft[Either[String, Vector[StoredAlert]]](Right(Vector.empty)) {
        case (Right(acc), Right(alert)) => Right(acc :+ alert)
        case (Left(error), _)           => Left(error)
        case (_, Left(error))           => Left(error)
      }
}

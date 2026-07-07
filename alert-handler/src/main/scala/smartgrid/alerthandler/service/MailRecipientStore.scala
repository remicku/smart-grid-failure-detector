package smartgrid.alerthandler.service

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, StandardOpenOption}

import cats.effect.IO
import cats.syntax.all._
import jakarta.mail.internet.InternetAddress

import scala.jdk.CollectionConverters._

final class MailRecipientStore(
    val path: Path,
    defaultRecipients: List[String]
) {
  def readRecipients: IO[Either[String, List[String]]] =
    if (Files.exists(path)) {
      IO.blocking(Files.readAllLines(path, StandardCharsets.UTF_8).asScala.toList)
        .attempt
        .map(_.leftMap(error => s"Cannot read ${path.toString}: ${error.getMessage}"))
        .map(_.map(normalize))
    } else {
      IO.pure(Right(normalize(defaultRecipients)))
    }

  def saveRecipients(rawRecipients: String): IO[Either[String, List[String]]] = {
    val recipients = parse(rawRecipients)

    if (recipients.isEmpty) {
      IO.pure(Left("At least one recipient email address is required."))
    } else {
      invalidRecipients(recipients) match {
        case invalid if invalid.nonEmpty =>
          IO.pure(Left(s"Invalid recipient email address(es): ${invalid.mkString(", ")}"))

        case _ =>
          writeRecipients(recipients).map(_.map(_ => recipients))
      }
    }
  }

  private def writeRecipients(recipients: List[String]): IO[Either[String, Unit]] =
    ensureParent *>
      IO.blocking {
        Files.write(
          path,
          (recipients.mkString(System.lineSeparator()) + System.lineSeparator())
            .getBytes(StandardCharsets.UTF_8),
          StandardOpenOption.CREATE,
          StandardOpenOption.TRUNCATE_EXISTING
        )
        ()
      }.attempt.map(_.leftMap(error => s"Cannot write ${path.toString}: ${error.getMessage}"))

  private def ensureParent: IO[Unit] =
    Option(path.getParent)
      .fold(IO.unit)(parent => IO.blocking(Files.createDirectories(parent)).void)

  private def parse(rawRecipients: String): List[String] =
    normalize(
      Option(rawRecipients)
        .getOrElse("")
        .split("[,;\\r\\n]+")
        .toList
    )

  private def normalize(values: List[String]): List[String] =
    values
      .map(_.trim)
      .filter(_.nonEmpty)
      .distinct

  private def invalidRecipients(recipients: List[String]): List[String] =
    recipients.filterNot(isValidEmail)

  private def isValidEmail(value: String): Boolean =
    Either
      .catchNonFatal {
        val address = new InternetAddress(value, true)
        address.validate()
        value.contains("@")
      }
      .toOption
      .contains(true)
}

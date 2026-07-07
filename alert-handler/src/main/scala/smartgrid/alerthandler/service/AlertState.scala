package smartgrid.alerthandler.service

import cats.effect.IO
import cats.effect.kernel.Ref
import smartgrid.alerthandler.model.StoredAlert

final case class AlertCounts(
    total: Int,
    warning: Int,
    critical: Int,
    unknown: Int
)

final class AlertState private (ref: Ref[IO, Vector[StoredAlert]]) {
  def add(alert: StoredAlert): IO[Unit] =
    ref.update(current => current :+ alert)

  def all: IO[Vector[StoredAlert]] =
    ref.modify(current => (current, current))

  def critical: IO[Vector[StoredAlert]] =
    all.map(_.filter(_.severity == "CRITICAL"))

  def byRegion(region: String): IO[Vector[StoredAlert]] = {
    val wanted = normalizeRegion(region)
    all.map(_.filter(alert => normalizeRegion(alert.source.region).equalsIgnoreCase(wanted)))
  }

  def counts: IO[AlertCounts] =
    all.map { alerts =>
      val warning = alerts.count(_.severity == "WARNING")
      val criticalCount = alerts.count(_.severity == "CRITICAL")
      AlertCounts(
        total = alerts.size,
        warning = warning,
        critical = criticalCount,
        unknown = alerts.size - warning - criticalCount
      )
    }

  def countsByRegion: IO[Map[String, Int]] =
    all.map(
      _.groupBy(alert => normalizeRegion(alert.source.region))
        .map { case (region, alerts) => region -> alerts.size }
    )

  private def normalizeRegion(value: String): String =
    Option(value).map(_.trim).filter(_.nonEmpty).getOrElse("unknown-region")
}

object AlertState {
  def from(alerts: Vector[StoredAlert]): IO[AlertState] =
    Ref.of[IO, Vector[StoredAlert]](alerts).map(ref => new AlertState(ref))
}

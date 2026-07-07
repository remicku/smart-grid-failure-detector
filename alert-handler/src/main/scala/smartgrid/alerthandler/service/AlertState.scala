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

final class AlertState private (ref: Ref[IO, Vector[StoredAlert]]) extends AlertRepository {
  override def init: IO[Either[String, Unit]] =
    IO.pure(Right(()))

  override def appendAlert(alert: StoredAlert): IO[Either[String, Unit]] =
    add(alert).as(Right(()))

  override def appendCriticalAlert(alert: StoredAlert): IO[Either[String, Unit]] =
    IO.pure(Right(()))

  def add(alert: StoredAlert): IO[Unit] =
    ref.update(current => current :+ alert)

  override def all: IO[Vector[StoredAlert]] =
    ref.modify(current => (current, current))

  override def critical: IO[Vector[StoredAlert]] =
    all.map(_.filter(_.severity == "CRITICAL"))

  override def byRegion(region: String): IO[Vector[StoredAlert]] = {
    val wanted = AlertRepository.normalizeRegion(region)
    all.map(_.filter(alert => AlertRepository.normalizeRegion(alert.source.region).equalsIgnoreCase(wanted)))
  }

  override def counts: IO[AlertCounts] =
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

  override def countsByRegion: IO[Map[String, Int]] =
    all.map(
      _.groupBy(alert => AlertRepository.normalizeRegion(alert.source.region))
        .map { case (region, alerts) => region -> alerts.size }
    )
}

object AlertState {
  def from(alerts: Vector[StoredAlert]): IO[AlertState] =
    Ref.of[IO, Vector[StoredAlert]](alerts).map(ref => new AlertState(ref))
}

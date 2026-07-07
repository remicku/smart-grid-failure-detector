package smartgrid.alerthandler.service

import cats.effect.IO
import smartgrid.alerthandler.model.StoredAlert

trait AlertRepository {
  def init: IO[Either[String, Unit]]
  def appendAlert(alert: StoredAlert): IO[Either[String, Unit]]
  def appendCriticalAlert(alert: StoredAlert): IO[Either[String, Unit]]
  def all: IO[Vector[StoredAlert]]
  def critical: IO[Vector[StoredAlert]]
  def byRegion(region: String): IO[Vector[StoredAlert]]
  def counts: IO[AlertCounts]
  def countsByRegion: IO[Map[String, Int]]
}

object AlertRepository {
  def normalizeRegion(value: String): String =
    Option(value).map(_.trim).filter(_.nonEmpty).getOrElse("unknown-region")
}

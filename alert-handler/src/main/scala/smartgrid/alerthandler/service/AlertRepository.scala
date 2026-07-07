package smartgrid.alerthandler.service

import cats.effect.IO
import smartgrid.alerthandler.model.StoredAlert

trait AlertRepository {
  def init: IO[Either[String, Unit]]
  def appendAlert(alert: StoredAlert): IO[Either[String, Unit]]
  def appendCriticalAlert(alert: StoredAlert): IO[Either[String, Unit]]
  def all: IO[Vector[StoredAlert]]
  def recent(limit: Int): IO[Vector[StoredAlert]]
  def critical: IO[Vector[StoredAlert]]
  def byRegion(region: String): IO[Vector[StoredAlert]]
  def counts: IO[AlertCounts]
  def countsByRegion: IO[Map[String, Int]]
  def claimMailNotification(alertId: String): IO[Either[String, Boolean]]
  def recordMailNotification(alertId: String, status: String, message: Option[String]): IO[Either[String, Unit]]
}

object AlertRepository {
  def normalizeRegion(value: String): String =
    Option(value).map(_.trim).filter(_.nonEmpty).getOrElse("unknown-region")
}

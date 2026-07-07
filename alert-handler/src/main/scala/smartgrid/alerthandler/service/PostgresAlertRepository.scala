package smartgrid.alerthandler.service

import scala.annotation.tailrec

import cats.effect.{IO, Resource}
import cats.syntax.all._
import io.circe.parser.decode
import io.circe.syntax._
import org.postgresql.ds.PGSimpleDataSource
import smartgrid.alerthandler.config.DatabaseConfig
import smartgrid.alerthandler.model.StoredAlert

final class PostgresAlertRepository(config: DatabaseConfig) extends AlertRepository {
  import PostgresAlertRepository._

  override def init: IO[Either[String, Unit]] =
    connection
      .use(conn =>
        Resource.make(IO.blocking(conn.createStatement()))(stmt =>
          IO.blocking(stmt.close()).handleError(_ => ())
        ).use(stmt =>
          initStatements.traverse_(sql =>
            IO.blocking {
              stmt.executeUpdate(sql)
              ()
            }
          )
        )
      )
      .attempt
      .map(_.leftMap(error => s"Cannot initialize PostgreSQL alert table: ${error.getMessage}"))

  override def appendAlert(alert: StoredAlert): IO[Either[String, Unit]] =
    connection
      .use(conn =>
        Resource.make(IO.blocking(conn.prepareStatement(insertAlertSql)))(stmt =>
          IO.blocking(stmt.close()).handleError(_ => ())
        ).use(stmt =>
          IO.blocking {
            stmt.setString(1, alert.alertId)
            stmt.setString(2, alert.severity)
            stmt.setLong(3, alert.detectedAt)
            stmt.setString(4, AlertRepository.normalizeRegion(alert.source.region))
            stmt.setString(5, alert.asJson.noSpaces)
            stmt.executeUpdate()
            ()
          }
        )
      )
      .attempt
      .map(_.leftMap(error => s"Cannot write alert to PostgreSQL: ${error.getMessage}"))

  override def appendCriticalAlert(alert: StoredAlert): IO[Either[String, Unit]] =
    IO.pure(Right(()))

  override def all: IO[Vector[StoredAlert]] =
    queryAlerts("SELECT payload_json FROM alerts ORDER BY detected_at DESC", None)

  override def critical: IO[Vector[StoredAlert]] =
    queryAlerts("SELECT payload_json FROM alerts WHERE severity = ? ORDER BY detected_at DESC", Some("CRITICAL"))

  override def byRegion(region: String): IO[Vector[StoredAlert]] =
    queryAlerts(
      "SELECT payload_json FROM alerts WHERE lower(region) = lower(?) ORDER BY detected_at DESC",
      Some(AlertRepository.normalizeRegion(region))
    )

  override def counts: IO[AlertCounts] =
    connection.use(conn =>
      Resource.make(IO.blocking(conn.prepareStatement(countSql)))(stmt =>
        IO.blocking(stmt.close()).handleError(_ => ())
      ).use(stmt =>
        Resource.make(IO.blocking(stmt.executeQuery()))(rs =>
          IO.blocking(rs.close()).handleError(_ => ())
        ).use(rs =>
          IO.blocking {
            if (rs.next()) {
              val total = rs.getInt("total")
              val warning = rs.getInt("warning")
              val critical = rs.getInt("critical")
              AlertCounts(
                total = total,
                warning = warning,
                critical = critical,
                unknown = total - warning - critical
              )
            } else {
              AlertCounts(total = 0, warning = 0, critical = 0, unknown = 0)
            }
          }
        )
      )
    )

  override def countsByRegion: IO[Map[String, Int]] =
    connection.use(conn =>
      Resource.make(IO.blocking(conn.prepareStatement(countsByRegionSql)))(stmt =>
        IO.blocking(stmt.close()).handleError(_ => ())
      ).use(stmt =>
        Resource.make(IO.blocking(stmt.executeQuery()))(rs =>
          IO.blocking(rs.close()).handleError(_ => ())
        ).use(rs =>
          IO.blocking {
            @tailrec
            def loop(acc: Map[String, Int]): Map[String, Int] =
              if (rs.next()) {
                loop(acc.updated(rs.getString("region"), rs.getInt("alert_count")))
              } else {
                acc
              }

            loop(Map.empty)
          }
        )
      )
    )

  private def queryAlerts(sql: String, parameter: Option[String]): IO[Vector[StoredAlert]] =
    connection.use(conn =>
      Resource.make(IO.blocking(conn.prepareStatement(sql)))(stmt =>
        IO.blocking(stmt.close()).handleError(_ => ())
      ).use(stmt =>
        IO.blocking(parameter.fold(())(value => stmt.setString(1, value))) *>
          Resource.make(IO.blocking(stmt.executeQuery()))(rs =>
            IO.blocking(rs.close()).handleError(_ => ())
          ).use(rs =>
            IO.blocking {
              @tailrec
              def loop(acc: Vector[StoredAlert]): Either[String, Vector[StoredAlert]] =
                if (rs.next()) {
                  decode[StoredAlert](rs.getString("payload_json")) match {
                    case Right(alert) => loop(acc :+ alert)
                    case Left(error)  => Left(s"Invalid alert payload from PostgreSQL: ${error.getMessage}")
                  }
                } else {
                  Right(acc)
                }

              loop(Vector.empty)
            }.flatMap(result => IO.fromEither(result.leftMap(message => new RuntimeException(message))))
          )
      )
    )

  private def connection =
    Resource.make(IO.blocking(dataSource.getConnection))(conn =>
      IO.blocking(conn.close()).handleError(_ => ())
    )

  private val dataSource: PGSimpleDataSource = {
    val source = new PGSimpleDataSource()
    source.setUrl(config.url)
    source.setUser(config.user)
    source.setPassword(config.password)
    source
  }
}

object PostgresAlertRepository {
  private val initStatements: List[String] =
    List(
      """CREATE TABLE IF NOT EXISTS alerts (
        |  alert_id TEXT PRIMARY KEY,
        |  severity TEXT NOT NULL,
        |  detected_at BIGINT NOT NULL,
        |  region TEXT NOT NULL,
        |  payload_json TEXT NOT NULL,
        |  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        |)""".stripMargin,
      "CREATE INDEX IF NOT EXISTS alerts_detected_at_idx ON alerts (detected_at DESC)",
      "CREATE INDEX IF NOT EXISTS alerts_region_idx ON alerts (region)",
      "CREATE INDEX IF NOT EXISTS alerts_severity_idx ON alerts (severity)"
    )

  private val insertAlertSql: String =
    """INSERT INTO alerts (alert_id, severity, detected_at, region, payload_json)
      |VALUES (?, ?, ?, ?, ?)
      |ON CONFLICT (alert_id) DO NOTHING""".stripMargin

  private val countSql: String =
    """SELECT
      |  COUNT(*)::int AS total,
      |  COUNT(*) FILTER (WHERE severity = 'WARNING')::int AS warning,
      |  COUNT(*) FILTER (WHERE severity = 'CRITICAL')::int AS critical
      |FROM alerts""".stripMargin

  private val countsByRegionSql: String =
    """SELECT region, COUNT(*)::int AS alert_count
      |FROM alerts
      |GROUP BY region
      |ORDER BY region""".stripMargin
}

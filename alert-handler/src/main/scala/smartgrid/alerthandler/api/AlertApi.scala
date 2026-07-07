package smartgrid.alerthandler.api

import java.net.{InetSocketAddress, URLDecoder}
import java.nio.charset.StandardCharsets
import java.util.Locale
import java.util.concurrent.Executors

import cats.effect.IO
import cats.effect.std.Dispatcher
import cats.syntax.all._
import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import io.circe.Encoder
import io.circe.generic.semiauto.deriveEncoder
import io.circe.parser.decode
import io.circe.syntax._
import smartgrid.alerthandler.config.ServerConfig
import smartgrid.alerthandler.service.{
  ActionFailed,
  ActionResult,
  ActionSucceeded,
  AlertBusinessLogic,
  AlertCounts,
  AlertRepository,
  MailService
}
import smartgrid.shared.AlertMessage

final class AlertApi(
    config: ServerConfig,
    kafkaTopic: String,
    repository: AlertRepository,
    mailService: MailService,
    businessLogic: AlertBusinessLogic,
    dispatcher: Dispatcher[IO]
) {
  import AlertApi._

  private val server: HttpServer =
    HttpServer.create(new InetSocketAddress(config.host, config.port), 0)

  private val executor =
    Executors.newFixedThreadPool(4)

  server.setExecutor(executor)
  server.createContext(
    "/",
    new HttpHandler {
      override def handle(exchange: HttpExchange): Unit =
        dispatcher.unsafeRunAndForget(
          route(exchange).handleErrorWith(error =>
            sendJson(exchange, 500, ErrorResponse(s"Internal server error: ${error.getMessage}"))
          )
        )
    }
  )

  def start: IO[Unit] =
    IO.blocking(server.start()) *>
      IO.println(s"[alert-handler] HTTP API started on http://${config.host}:${config.port}")

  def stop: IO[Unit] =
    IO.blocking {
      server.stop(0)
      executor.shutdown()
      ()
    }

  private def route(exchange: HttpExchange): IO[Unit] = {
    val method = Option(exchange.getRequestMethod).getOrElse("GET").toUpperCase(Locale.ROOT)
    val path = Option(exchange.getRequestURI).map(_.getPath).getOrElse("/")

    (method, path) match {
      case ("GET", "/" | "/dashboard") =>
        (
          repository.recent(DashboardAlertLimit),
          repository.counts,
          repository.countsByRegion,
          mailService.readRecipients
        ).tupled.flatMap { case (alerts, counts, countsByRegion, recipientsResult) =>
          recipientsResult.fold(
            error => sendHtml(exchange, 500, errorPage("Cannot load recipients", error)),
            recipients =>
              sendHtml(
                exchange,
                200,
                DashboardRenderer.render(
                  alerts,
                  counts,
                  countsByRegion,
                  recipients,
                  mailService.recipientsPath.toString
                )
              )
          )
        }

      case ("POST", "/mail/recipients") =>
        readRequestBody(exchange)
          .flatMap(
            _.fold(
              error => IO.pure(Left(error)),
              body => IO.pure(formField(body, "recipients").toRight("Missing form field: recipients"))
            )
          )
          .flatMap(
            _.fold(
              error => IO.pure(Left(error)),
              mailService.saveRecipients
            )
          )
          .flatMap(
            _.fold(
              error => sendHtml(exchange, 400, errorPage("Recipients not saved", error)),
              _ => redirect(exchange, "/dashboard")
            )
          )

      case ("GET", "/mail/recipients") =>
        mailService.readRecipients.flatMap(
          _.fold(
            error => sendJson(exchange, 500, ErrorResponse(error)),
            recipients => sendJson(exchange, 200, RecipientsResponse(recipients))
          )
        )

      case ("GET", "/health") =>
        repository.counts.flatMap(counts =>
          sendJson(
            exchange,
            200,
            HealthResponse(
              status = "ok",
              service = "alert-handler",
              kafkaTopic = kafkaTopic,
              totalAlerts = counts.total
            )
          )
        )

      case ("GET", "/alerts") =>
        repository.all.flatMap(alerts => sendJson(exchange, 200, alerts))

      case ("POST", "/dev/alerts") =>
        readRequestBody(exchange)
          .flatMap(
            _.fold(
              error => IO.pure(Left(error)),
              body => IO.pure(decode[AlertMessage](body).leftMap(error => error.getMessage))
            )
          )
          .flatMap(
            _.fold(
              error => sendJson(exchange, 400, ErrorResponse(error)),
              alert =>
                businessLogic
                  .handle(alert)
                  .flatMap(results => sendJson(exchange, 202, IngestResponse("processed", resultViews(results))))
            )
          )

      case ("GET", "/alerts/critical") =>
        repository.critical.flatMap(alerts => sendJson(exchange, 200, alerts))

      case ("GET", "/alerts/count") =>
        repository.counts.flatMap(counts => sendJson(exchange, 200, counts))

      case ("GET", "/notifications") =>
        mailService.readNotifications.flatMap(
          _.fold(
            error => sendJson(exchange, 500, ErrorResponse(error)),
            notifications => sendJson(exchange, 200, NotificationsResponse(notifications))
          )
        )

      case ("GET", regionPath) if regionPath.startsWith("/alerts/by-region/") =>
        val encodedRegion = regionPath.stripPrefix("/alerts/by-region/")
        val region = URLDecoder.decode(encodedRegion, StandardCharsets.UTF_8.name())
        repository.byRegion(region).flatMap(alerts => sendJson(exchange, 200, alerts))

      case ("GET", regionPath) if regionPath.startsWith("/alerts/by-zone/") =>
        val encodedRegion = regionPath.stripPrefix("/alerts/by-zone/")
        val region = URLDecoder.decode(encodedRegion, StandardCharsets.UTF_8.name())
        repository.byRegion(region).flatMap(alerts => sendJson(exchange, 200, alerts))

      case ("GET", _) =>
        sendJson(exchange, 404, ErrorResponse(s"Unknown route: $path"))

      case _ =>
        sendJson(exchange, 405, ErrorResponse("Method not allowed on this route."))
    }
  }

  private def sendJson[A: Encoder](exchange: HttpExchange, status: Int, payload: A): IO[Unit] =
    send(exchange, status, "application/json; charset=utf-8", payload.asJson.spaces2)

  private def sendHtml(exchange: HttpExchange, status: Int, payload: String): IO[Unit] =
    send(exchange, status, "text/html; charset=utf-8", payload)

  private def send(exchange: HttpExchange, status: Int, contentType: String, payload: String): IO[Unit] =
    IO.blocking {
      val bytes = payload.getBytes(StandardCharsets.UTF_8)
      exchange.getResponseHeaders.set("Content-Type", contentType)
      exchange.sendResponseHeaders(status, bytes.length.toLong)
      exchange.getResponseBody.write(bytes)
      exchange.getResponseBody.close()
      ()
    }

  private def redirect(exchange: HttpExchange, location: String): IO[Unit] =
    IO.blocking {
      exchange.getResponseHeaders.set("Location", location)
      exchange.sendResponseHeaders(303, -1L)
      exchange.close()
      ()
    }

  private def readRequestBody(exchange: HttpExchange): IO[Either[String, String]] =
    IO.blocking(new String(exchange.getRequestBody.readAllBytes(), StandardCharsets.UTF_8))
      .attempt
      .map(_.leftMap(error => s"Cannot read request body: ${error.getMessage}"))

  private def formField(body: String, name: String): Option[String] =
    body
      .split("&")
      .toList
      .flatMap(pair =>
        pair.split("=", 2).toList match {
          case key :: value :: Nil if decodeForm(key) == name => Some(decodeForm(value))
          case key :: Nil if decodeForm(key) == name          => Some("")
          case _                                             => None
        }
      )
      .headOption

  private def decodeForm(value: String): String =
    URLDecoder.decode(value, StandardCharsets.UTF_8.name())

  private def errorPage(title: String, message: String): String =
    s"""<!doctype html>
       |<html lang="en">
       |<head><meta charset="utf-8"><title>${html(title)}</title></head>
       |<body>
       |  <h1>${html(title)}</h1>
       |  <p>${html(message)}</p>
       |  <p><a href="/dashboard">Back to dashboard</a></p>
       |</body>
       |</html>""".stripMargin

  private def html(value: String): String =
    Option(value)
      .getOrElse("")
      .replace("&", "&amp;")
      .replace("<", "&lt;")
      .replace(">", "&gt;")
      .replace("\"", "&quot;")
      .replace("'", "&#39;")
}

object AlertApi {
  private val DashboardAlertLimit: Int = 500

  final case class HealthResponse(
      status: String,
      service: String,
      kafkaTopic: String,
      totalAlerts: Int
  )

  final case class ErrorResponse(error: String)
  final case class NotificationsResponse(notifications: List[String])
  final case class RecipientsResponse(recipients: List[String])

  final case class ActionResultView(
      actionName: String,
      success: Boolean,
      message: Option[String]
  )

  final case class IngestResponse(
      status: String,
      actions: List[ActionResultView]
  )

  private def resultViews(results: List[ActionResult]): List[ActionResultView] =
    results.map {
      case ActionSucceeded(actionName) =>
        ActionResultView(actionName, success = true, message = None)

      case ActionFailed(actionName, message) =>
        ActionResultView(actionName, success = false, message = Some(message))
    }

  implicit val alertCountsEncoder: Encoder[AlertCounts] = deriveEncoder
  implicit val healthResponseEncoder: Encoder[HealthResponse] = deriveEncoder
  implicit val errorResponseEncoder: Encoder[ErrorResponse] = deriveEncoder
  implicit val notificationsResponseEncoder: Encoder[NotificationsResponse] = deriveEncoder
  implicit val recipientsResponseEncoder: Encoder[RecipientsResponse] = deriveEncoder
  implicit val actionResultViewEncoder: Encoder[ActionResultView] = deriveEncoder
  implicit val ingestResponseEncoder: Encoder[IngestResponse] = deriveEncoder
}

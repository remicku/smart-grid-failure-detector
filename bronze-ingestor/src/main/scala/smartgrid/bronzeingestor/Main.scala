package smartgrid.bronzeingestor

import java.net.URI
import java.nio.charset.StandardCharsets

import cats.effect.{IO, IOApp, Resource}
import cats.effect.std.Console
import fs2.Stream
import fs2.kafka._
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.PutObjectRequest

object Main extends IOApp.Simple {

  private val bootstrap = env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092,localhost:9094,localhost:9096")
  private val topic = env("TELEMETRY_TOPIC", "ST")
  private val groupId = env("BRONZE_GROUP_ID", "bronze-ingestor")
  private val output = ObjectStorePath.from(env("BRONZE_OUTPUT", "s3://smartgrid-lake/bronze/telemetry"))
  private val maxMessages = sys.env.lift("BRONZE_MAX_MESSAGES").flatMap(_.toLongOption)
  private val s3Endpoint = env("S3_ENDPOINT", "http://localhost:9000")
  private val s3AccessKey = env("S3_ACCESS_KEY", "smartgrid")
  private val s3SecretKey = env("S3_SECRET_KEY", "smartgrid")
  private val s3Region = env("S3_REGION", "us-east-1")

  private val settings =
    ConsumerSettings[IO, Array[Byte], String]
      .withBootstrapServers(bootstrap)
      .withGroupId(groupId)
      .withAutoOffsetReset(AutoOffsetReset.Earliest)

  private def env(name: String, fallback: String): String =
    sys.env.lift(name).filter(_.nonEmpty).getOrElse(fallback)

  private val s3Client: Resource[IO, S3Client] =
    Resource.make(
      IO.blocking(
        S3Client
          .builder()
          .endpointOverride(URI.create(s3Endpoint))
          .credentialsProvider(
            StaticCredentialsProvider.create(AwsBasicCredentials.create(s3AccessKey, s3SecretKey))
          )
          .region(Region.of(s3Region))
          .forcePathStyle(true)
          .build()
      )
    )(client => IO.blocking(client.close()).handleError(_ => ()))

  private def writeBronzeObject(raw: String, key: String, client: S3Client): IO[Unit] =
    IO.blocking {
      val request =
        PutObjectRequest
          .builder()
          .bucket(output.bucket)
          .key(key)
          .contentType("application/json")
          .build()
      val body = RequestBody.fromString(raw.replace("\n", "").replace("\r", ""), StandardCharsets.UTF_8)
      client.putObject(request, body)
      ()
    }

  private def objectKey(record: ConsumerRecord[Array[Byte], String]): String =
    s"${output.prefix}/partition=${record.partition}/offset=${record.offset}.json"

  private def limit[A](stream: Stream[IO, A]): Stream[IO, A] =
    maxMessages.fold(stream)(count => stream.take(count))

  val run: IO[Unit] =
    s3Client.use { client =>
      limit(
        KafkaConsumer
          .stream(settings)
          .subscribeTo(topic)
          .records
      )
        .evalMap { record =>
          val key = objectKey(record.record)
          writeBronzeObject(record.record.value, key, client) >>
            record.offset.commit >>
            Console[IO].println(s"[bronze] wrote one message to s3://${output.bucket}/$key")
        }
        .compile
        .drain
    }

  private final case class ObjectStorePath(bucket: String, prefix: String)

  private object ObjectStorePath {
    def from(value: String): ObjectStorePath = {
      val uri = URI.create(value)
      val bucket = Option(uri.getHost).filter(_.nonEmpty).getOrElse("smartgrid-lake")
      val prefix = Option(uri.getPath)
        .map(_.stripPrefix("/").stripSuffix("/"))
        .filter(_.nonEmpty)
        .getOrElse("bronze/telemetry")
      ObjectStorePath(bucket, prefix)
    }
  }
}

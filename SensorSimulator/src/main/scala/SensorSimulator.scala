import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.time.Instant
import java.util.Properties
import scala.util.Random

final case class SensorReading(
    sensorId: String,
    transformerId: String,
    timestamp: String,
    region: String,
    voltage: Double,
    current: Double,
    temperature: Double,
    load: Double,
    failureRiskScore: Double,
    status: String
)

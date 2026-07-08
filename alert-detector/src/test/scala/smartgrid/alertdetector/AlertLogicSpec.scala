package smartgrid.alertdetector

import smartgrid.shared.SensorMessage

class AlertLogicSpec extends munit.FunSuite {

  private def sensor(score: Double): SensorMessage =
    SensorMessage(
      sensorId = "s1",
      transformerId = "t1",
      timestamp = "1970-01-01T00:00:00Z",
      region = "R1",
      voltage = 230.0,
      current = 10.0,
      temperature = 40.0,
      load = 50.0,
      failureRiskScore = score,
      status = "ONLINE"
    )

  test("no alert") {
    assertEquals(AlertLogic.severityFor(0.5), None)
  }

  test("WARNING test") {
    assertEquals(AlertLogic.severityFor(0.85), Some("WARNING"))
  }

  test("CRITICAL test") {
    assertEquals(AlertLogic.severityFor(0.95), Some("CRITICAL"))
  }

  test("severity score") {
    val r = AlertLogic.reasonFor(sensor(0.95), "CRITICAL")
    assert(r.contains("0.95"))
    assert(r.contains("CRITICAL"))
  }

  test("spike is held") {
    assertEquals(AlertLogic.evaluate(None, 0.85, 1000L, 30000L), (Some(1000L), None))
  }

  test("streak start kept") {
    assertEquals(AlertLogic.evaluate(Some(1000L), 0.85, 2000L, 30000L), (Some(1000L), None))
  }

  test("sustained warning") {
    assertEquals(AlertLogic.evaluate(Some(0L), 0.85, 30000L, 30000L), (Some(0L), Some("WARNING")))
  }

  test("critical fires now") {
    assertEquals(AlertLogic.evaluate(None, 0.95, 0L, 30000L), (Some(0L), Some("CRITICAL")))
  }

  test("streak reset below threshold") {
    assertEquals(AlertLogic.evaluate(Some(0L), 0.5, 5000L, 30000L), (None, None))
  }
}

package smartgrid.alertdetector

import smartgrid.shared.SensorMessage

class AlertLogicSpec extends munit.FunSuite {

  private def sensor(score: Double): SensorMessage =
    SensorMessage(
      sensorId = "s1",
      transformerId = "t1",
      timestamp = 0L,
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
}

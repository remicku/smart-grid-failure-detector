package smartgrid.alerthandler.model

import smartgrid.alerthandler.model.AlertSeverity.{Critical, UnknownSeverity, Warning}

class AlertSeveritySpec extends munit.FunSuite {
  test("parse WARNING severity") {
    assertEquals(AlertSeverity.parse("WARNING"), Warning)
    assertEquals(AlertSeverity.parse(" warning "), Warning)
  }

  test("parse CRITICAL severity") {
    assertEquals(AlertSeverity.parse("CRITICAL"), Critical)
    assertEquals(AlertSeverity.parse("critical"), Critical)
  }

  test("parse unknown severity") {
    assertEquals(AlertSeverity.parse("minor"), UnknownSeverity("MINOR"))
  }
}

package smartgrid.alerthandler.api

import smartgrid.alerthandler.model.StoredAlert
import smartgrid.alerthandler.service.AlertCounts

object DashboardRenderer {
  def render(
      alerts: Vector[StoredAlert],
      counts: AlertCounts,
      countsByRegion: Map[String, Int],
      recipients: List[String],
      recipientsPath: String
  ): String = {
    val latestRows = alerts
      .sortBy(_.detectedAt)
      .reverse
      .take(10)
      .map(alert =>
        s"""<tr>
           |<td>${escape(alert.alertId)}</td>
           |<td><span class="badge ${severityClass(alert.severity)}">${escape(alert.severity)}</span></td>
           |<td>${escape(region(alert))}</td>
           |<td>${escape(alert.source.transformerId)}</td>
           |<td>${escape(alert.reason)}</td>
           |<td>${alert.detectedAt}</td>
           |</tr>""".stripMargin
      )
      .mkString(System.lineSeparator())

    val regionRows = countsByRegion.toList
      .sortBy { case (regionName, _) => regionName }
      .map { case (regionName, count) =>
        s"<li><span>${escape(regionName)}</span><strong>$count</strong></li>"
      }
      .mkString(System.lineSeparator())

    s"""<!doctype html>
       |<html lang="en">
       |<head>
       |  <meta charset="utf-8">
       |  <meta name="viewport" content="width=device-width, initial-scale=1">
       |  <title>Smart Grid Alert Handler</title>
       |  <style>
       |    :root {
       |      color-scheme: light;
       |      font-family: Arial, Helvetica, sans-serif;
       |      background: #f5f7f9;
       |      color: #16202a;
       |    }
       |    body { margin: 0; }
       |    header {
       |      background: #12343b;
       |      color: white;
       |      padding: 24px max(20px, 6vw);
       |    }
       |    main {
       |      max-width: 1120px;
       |      margin: 0 auto;
       |      padding: 24px 20px 40px;
       |    }
       |    h1 { margin: 0 0 6px; font-size: 28px; letter-spacing: 0; }
       |    h2 { margin-top: 32px; font-size: 20px; letter-spacing: 0; }
       |    .subtitle { margin: 0; color: #c8d7dc; }
       |    .stats {
       |      display: grid;
       |      grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
       |      gap: 12px;
       |    }
       |    .stat {
       |      background: white;
       |      border: 1px solid #dce3e8;
       |      border-radius: 8px;
       |      padding: 16px;
       |    }
       |    .stat span { color: #5f6f7a; font-size: 13px; }
       |    .stat strong { display: block; font-size: 28px; margin-top: 6px; }
       |    table {
       |      width: 100%;
       |      border-collapse: collapse;
       |      background: white;
       |      border: 1px solid #dce3e8;
       |      border-radius: 8px;
       |      overflow: hidden;
       |    }
       |    th, td {
       |      padding: 10px 12px;
       |      text-align: left;
       |      border-bottom: 1px solid #edf1f4;
       |      vertical-align: top;
       |    }
       |    th { background: #edf3f5; color: #35444f; font-size: 13px; }
       |    tr:last-child td { border-bottom: 0; }
       |    .badge {
       |      display: inline-block;
       |      min-width: 76px;
       |      border-radius: 6px;
       |      padding: 4px 8px;
       |      font-size: 12px;
       |      font-weight: 700;
       |      text-align: center;
       |    }
       |    .critical { background: #ffe2df; color: #a92318; }
       |    .warning { background: #fff0cf; color: #825900; }
       |    .unknown { background: #e8eaee; color: #3d4752; }
       |    .region-list {
       |      display: grid;
       |      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
       |      gap: 8px;
       |      padding: 0;
       |      list-style: none;
       |    }
       |    .region-list li {
       |      display: flex;
       |      justify-content: space-between;
       |      gap: 12px;
       |      background: white;
       |      border: 1px solid #dce3e8;
       |      border-radius: 8px;
       |      padding: 10px 12px;
       |    }
       |    .links {
       |      display: flex;
       |      flex-wrap: wrap;
       |      gap: 10px;
       |      margin-top: 16px;
       |    }
       |    .links a {
       |      color: #0d5d76;
       |      background: white;
       |      border: 1px solid #cbd7dd;
       |      border-radius: 6px;
       |      padding: 8px 10px;
       |      text-decoration: none;
       |      font-size: 14px;
       |    }
       |    .settings-form {
       |      display: grid;
       |      gap: 10px;
       |      max-width: 620px;
       |    }
       |    label {
       |      color: #35444f;
       |      font-size: 13px;
       |      font-weight: 700;
       |    }
       |    textarea {
       |      min-height: 120px;
       |      resize: vertical;
       |      border: 1px solid #cbd7dd;
       |      border-radius: 8px;
       |      padding: 10px 12px;
       |      font: inherit;
       |      line-height: 1.45;
       |    }
       |    button {
       |      justify-self: start;
       |      border: 0;
       |      border-radius: 6px;
       |      background: #12343b;
       |      color: white;
       |      padding: 9px 14px;
       |      font: inherit;
       |      font-weight: 700;
       |      cursor: pointer;
       |    }
       |    .hint {
       |      margin: 0;
       |      color: #5f6f7a;
       |      font-size: 13px;
       |    }
       |  </style>
       |</head>
       |<body>
       |  <header>
       |    <h1>Smart Grid Alert Handler</h1>
       |    <p class="subtitle">Kafka ST2 alert handling, PostgreSQL storage and SMTP notifications.</p>
       |  </header>
       |  <main>
       |    <section class="stats">
       |      <div class="stat"><span>Total alerts</span><strong>${counts.total}</strong></div>
       |      <div class="stat"><span>Critical alerts</span><strong>${counts.critical}</strong></div>
       |      <div class="stat"><span>Warning alerts</span><strong>${counts.warning}</strong></div>
       |      <div class="stat"><span>Unknown severity</span><strong>${counts.unknown}</strong></div>
       |    </section>
       |
       |    <section>
       |      <h2>Latest Alerts</h2>
       |      <table>
       |        <thead>
       |          <tr><th>Alert id</th><th>Severity</th><th>Region</th><th>Transformer</th><th>Reason</th><th>Detected at</th></tr>
       |        </thead>
       |        <tbody>
       |          ${if (latestRows.nonEmpty) latestRows else "<tr><td colspan=\"6\">No alert processed yet.</td></tr>"}
       |        </tbody>
       |      </table>
       |    </section>
       |
       |    <section>
       |      <h2>Alerts By Region</h2>
       |      <ul class="region-list">
       |        ${if (regionRows.nonEmpty) regionRows else "<li><span>No region yet</span><strong>0</strong></li>"}
       |      </ul>
       |    </section>
       |
       |    <section>
       |      <h2>Email Recipients</h2>
       |      <form class="settings-form" method="post" action="/mail/recipients">
       |        <label>Destination addresses</label>
       |        <textarea id="recipients" name="recipients">${escape(recipients.mkString(System.lineSeparator()))}</textarea>
       |        <p class="hint">${escape(recipientsPath)}</p>
       |        <button type="submit">Save recipients</button>
       |      </form>
       |    </section>
       |
       |    <section>
       |      <h2>API</h2>
       |      <div class="links">
       |        <a href="/health">/health</a>
       |        <a href="/alerts">/alerts</a>
       |        <a href="/alerts/critical">/alerts/critical</a>
       |        <a href="/alerts/count">/alerts/count</a>
       |        <a href="/notifications">/notifications</a>
       |        <a href="/mail/recipients">/mail/recipients</a>
       |      </div>
       |    </section>
       |  </main>
       |</body>
       |</html>""".stripMargin
  }

  private def severityClass(value: String): String =
    value match {
      case "CRITICAL" => "critical"
      case "WARNING"  => "warning"
      case _          => "unknown"
    }

  private def region(alert: StoredAlert): String =
    Option(alert.source.region).map(_.trim).filter(_.nonEmpty).getOrElse("unknown-region")

  private def escape(value: String): String =
    Option(value)
      .getOrElse("")
      .replace("&", "&amp;")
      .replace("<", "&lt;")
      .replace(">", "&gt;")
      .replace("\"", "&quot;")
      .replace("'", "&#39;")
}

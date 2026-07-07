package smartgrid.alerthandler.service

sealed trait ActionResult {
  def actionName: String
}

final case class ActionSucceeded(actionName: String) extends ActionResult
final case class ActionFailed(actionName: String, message: String) extends ActionResult

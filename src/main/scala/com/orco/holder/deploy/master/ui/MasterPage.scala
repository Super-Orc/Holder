package com.orco.holder.deploy.master.ui


import javax.servlet.http.HttpServletRequest

import com.orco.holder.deploy.DeployMessages.{MasterStateResponse, RequestMasterState}
import com.orco.holder.deploy.JsonProtocol
import com.orco.holder.deploy.master.MasterStandByInfo
import com.orco.holder.executor.ContainerInfo
import com.orco.holder.ui.{UIUtils, WebUIPage}
import com.orco.holder.util.Utils
import javax.servlet.http.HttpServletRequest


import org.json4s.JValue

import scala.xml.Node

/**
  * parent 可能是 master 或者 standby
  */
private[ui] class MasterPage(parent: MasterWebUI) extends WebUIPage("") {
  private val master = parent.masterEndpointRef

  def getMasterState: MasterStateResponse = {
    master.askSync[MasterStateResponse](RequestMasterState)
  }

  override def renderJson(request: HttpServletRequest): JValue = {
    println(1111122223)
    JsonProtocol.writeMasterState(getMasterState)
  }

  def handleAppKillRequest(request: HttpServletRequest): Unit = {
    handleKillRequest(request, id => {
      Option(parent.master.containers().get(id)).foreach { app =>
        parent.master.stopContainer(app.message)
      }
    })
  }

  //  def handleDriverKillRequest(request: HttpServletRequest): Unit = {
  //    handleKillRequest(request, id => {
  //      master.ask[KillDriverResponse](RequestKillDriver(id))
  //    })
  //  }

  private def handleKillRequest(request: HttpServletRequest, action: String => Unit): Unit = {
    if (parent.killEnabled
    //      &&      parent.master.securityMgr.checkModifyPermissions(request.getRemoteUser)
    ) {
      // stripXSS is called first to remove suspicious characters used in XSS attacks
      val killFlag = Option(UIUtils.stripXSS(request.getParameter("terminate"))).getOrElse("false").toBoolean
      val id = Option(UIUtils.stripXSS(request.getParameter("id")))
      if (id.isDefined && killFlag) {
        action(id.get.substring(10).split("-")(0))
      }

      Thread.sleep(100)
    }
  }

  /** Index view listing applications and executors */
  def render(request: HttpServletRequest): Seq[Node] = {
    val state = getMasterState
    var sbHeaders: Seq[String] = null
    var aliveWorkers: Array[MasterStandByInfo] = null
    var workerTable: Seq[Node] = null
    if (state.isMaster) {
      sbHeaders = Seq("Standby Id", "Address", "State", "Cores")
      aliveWorkers = state.standBys.sortBy(_.id)
      workerTable = UIUtils.listingTable(sbHeaders, workerRow, aliveWorkers)
    }
    // TODO: state.masterWebUI可以去掉了
    val appHeaders = Seq("Application ID", "Name", "Memory per Executor", "Submitted Time",
      "User", "State", "Duration", "Logs")
    val activeApps = state.activeApps.filter(a => a.nodeId.contains(s"${state.host}-${state.port}")).sortBy(_.startTime).reverse
    val activeAppsTable = UIUtils.listingTable(appHeaders, appRow, activeApps)
    val completedApps = state.completeApps.filter(a => a.nodeId.contains(s"${state.host}-${state.port}")).sortBy(_.endTime).reverse
    val completedAppsTable = UIUtils.listingTable(appHeaders, appRow, completedApps)

    //    val driverHeaders = Seq("Submission ID", "Submitted Time", "Worker", "State", "Cores",
    //      "Memory", "Main Class")
    //    val activeDrivers = state.activeDrivers.sortBy(_.startTime).reverse
    //    val activeDriversTable = UIUtils.listingTable(driverHeaders, driverRow, activeDrivers)
    //    val completedDrivers = state.completedDrivers.sortBy(_.startTime).reverse
    //    val completedDriversTable = UIUtils.listingTable(driverHeaders, driverRow, completedDrivers)

    // For now we only show driver information if the user has submitted drivers to the cluster.
    // This is until we integrate the notion of drivers and applications in the UI.

    val content =
      if (state.isMaster) {

        <div class="row-fluid">
          <div class="span12">
            <ul class="unstyled">
              <li>
                <strong>Alive Master-Standbys:</strong>{aliveWorkers.length}
              </li>
              <li>
                <strong>Applications:</strong>{activeApps.length}
              </li>
            </ul>
          </div>
        </div>

          <div class="row-fluid">
            <div class="span12">
              <h4>Master-Standbys (
                {aliveWorkers.length}
                )</h4>{workerTable}
            </div>
          </div>

          <div class="row-fluid">
            <div class="span12">
              <h4 id="running-app">Running Applications (
                {activeApps.length}
                )</h4>{activeAppsTable}
            </div>
          </div>

          <div class="row-fluid">
            <div class="span12">
              <h4 id="completed-app">Completed Applications (
                {completedApps.length}
                )</h4>{completedAppsTable}
            </div>
          </div>
      } else {
        <div class="row-fluid">
          <div class="span12">
            <ul class="unstyled">
              <p>
                <a href={state.masterWebUI}>Back to Master</a>
              </p>
            </ul>
          </div>
        </div>


          <div class="row-fluid">
            <div class="span12">
              <ul class="unstyled">
                <li>
                  <strong>Applications:</strong>{activeApps.length}
                </li>
              </ul>
            </div>
          </div>


          <div class="row-fluid">
            <div class="span12">
              <h4 id="running-app">Running Applications (
                {activeApps.length}
                )</h4>{activeAppsTable}
            </div>
          </div>

          <div class="row-fluid">
            <div class="span12">
              <h4 id="completed-app">Completed Applications (
                {completedApps.length}
                )</h4>{completedAppsTable}
            </div>
          </div>
      }

    UIUtils.basicSparkPage(content, "Applications Holder")
  }

  private def workerRow(standBy: MasterStandByInfo): Seq[Node] = {
    <tr>
      <td>
        <a href={standBy.webUiAddress}>
          {standBy.id}
        </a>
      </td>
      <td>
        {standBy.host}
        :
        {standBy.port}
      </td>
      <td>
        {"state"}
      </td>
      <td>
        {"cores"}
      </td>
    </tr>
  }

  private def appRow(app: ContainerInfo): Seq[Node] = {
//    val workerUrlRef = s"http://${app.nodeId.split("-").tail.tail.mkString(":")}"
//    val a = app.executor.master.asInstanceOf[Master].webUiUrl
    val killLink = if (parent.killEnabled && app.state != "FINISHED") {
      val confirm =
        s"if (window.confirm('Are you sure you want to kill application ${app.id} ?')) " +
          "{ this.parentNode.submit(); return true; } else { return false; }"
      <form action="app/kill/" method="POST" style="display:inline">
        <input type="hidden" name="id" value={app.id}/>
        <input type="hidden" name="terminate" value="true"/>
        <a href="#" onclick={confirm} class="kill-link">(kill)</a>
      </form>
    }
    <tr>
      <td>
        {app.id}{killLink}
      </td>
      <td>
        {app.name}
      </td>
      <td sorttable_customkey={app.memory.toString}>
        {Utils.megabytesToString(app.memory)}
      </td>
      <td>
        {UIUtils.formatDate(app.submitDate)}
      </td>
      <td>
        {app.nodeId}
      </td>
      <td>
        {app.state}
      </td>
      <td>
        {UIUtils.formatDuration(app.duration)}
      </td>
      <td>
        <a href={s"${app.webUiUrl}/logPage?appId=${app.fileName}&logType=stdout"}>stdout</a>
        <a href={s"${app.webUiUrl}/logPage?appId=${app.fileName}&logType=stderr"}>stderr</a>
      </td>
    </tr>
  }

}

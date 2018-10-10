package com.orco.holder.deploy.master.ui


import java.io.File
import javax.servlet.http.HttpServletRequest

import com.orco.holder.deploy.master.Master
import com.orco.holder.internal.Logging
import com.orco.holder.rpc.RpcEndpointRef
import com.orco.holder.ui.WebUI
import com.orco.holder.ui.JettyUtils._
/**
  * Web UI server for the standalone master.
  */
private[master]
class MasterWebUI(
                   val master: Master,
                   val workDir: File,
                   requestedPort: Int)
  extends WebUI(requestedPort, master.conf, name = "MasterUI") with Logging {

  val masterEndpointRef: RpcEndpointRef = master.self
  val killEnabled: Boolean = master.conf.getBoolean("holder.ui.killEnabled", defaultValue = true)

  initialize()

  /** Initialize all components of the server. */
  def initialize() {
    val masterPage = new MasterPage(this)
    val logPage = new LogPage(this)
    //    attachPage(new ApplicationPage(this))
    attachPage(masterPage)
    attachPage(logPage)
    attachHandler(createStaticHandler(MasterWebUI.STATIC_RESOURCE_DIR, "/static"))
    attachHandler(createRedirectHandler("/app/kill", "/", masterPage.handleAppKillRequest, httpMethods = Set("POST")))
    attachHandler(createServletHandler("/log", (request: HttpServletRequest) => logPage.renderLog(request), master.conf))
  }

  //  def addProxy(): Unit = {
  //    val handler = createProxyHandler(idToUiAddress)
  //    attachHandler(handler)
  //  }
  //
  //  def idToUiAddress(id: String): Option[String] = {
  //    val state = masterEndpointRef.askSync[MasterStateResponse](RequestMasterState)
  //    val maybeWorkerUiAddress = state.workers.find(_.id == id).map(_.webUiAddress)
  //    val maybeAppUiAddress = state.activeApps.find(_.id == id).map(_.desc.appUiUrl)
  //
  //    maybeWorkerUiAddress.orElse(maybeAppUiAddress)
  //  }

}

private[master] object MasterWebUI {
  private val STATIC_RESOURCE_DIR = "ui/static"
}
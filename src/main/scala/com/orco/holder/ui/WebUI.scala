package com.orco.holder.ui

import javax.servlet.http.HttpServletRequest

import com.orco.holder.HolderConf
import com.orco.holder.internal.Logging
import com.orco.holder.ui.JettyUtils._
import com.orco.holder.util.Utils
import org.eclipse.jetty.servlet.ServletContextHandler
import org.json4s.JsonAST.{JNothing, JValue}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.xml.Node

/**
  * The top level component of the UI hierarchy that contains the server.
  *
  * Each WebUI represents a collection of tabs, each of which in turn represents a collection of
  * pages. The use of tabs is optional, however; a WebUI may choose to include pages directly.
  */
private[holder] abstract class WebUI(
                                     //                                     val securityManager: SecurityManager,
                                     //                                     val sslOptions: SSLOptions,
                                     port: Int,
                                     conf: HolderConf,
                                     basePath: String = "",
                                     name: String = "")
  extends Logging {

  protected val tabs: ArrayBuffer[WebUITab] = ArrayBuffer[WebUITab]()
  protected val handlers: ArrayBuffer[ServletContextHandler] = ArrayBuffer[ServletContextHandler]()
  protected val pageToHandlers = new mutable.HashMap[WebUIPage, ArrayBuffer[ServletContextHandler]]
  protected var serverInfo: Option[ServerInfo] = None
  // TODO: 临时的
  protected val publicHostName: String = Utils.localCanonicalHostName()
  private val className = Utils.getFormattedClassName(this)

  def getBasePath: String = basePath

  def getTabs: Seq[WebUITab] = tabs

  def getHandlers: Seq[ServletContextHandler] = handlers

  //  def getSecurityManager: SecurityManager = securityManager

  /** Attach a tab to this UI, along with all of its attached pages. */
  def attachTab(tab: WebUITab) {
    tab.pages.foreach(attachPage)
    tabs += tab
  }

  def detachTab(tab: WebUITab) {
    tab.pages.foreach(detachPage)
    tabs -= tab
  }

  def detachPage(page: WebUIPage) {
    pageToHandlers.remove(page).foreach(_.foreach(detachHandler))
  }

  /** Attach a page to this UI. */
  def attachPage(page: WebUIPage) {
    val pagePath = "/" + page.prefix
    val renderHandler = createServletHandler(pagePath,
      (request: HttpServletRequest) => page.render(request), conf, basePath)
    val renderJsonHandler = createServletHandler(pagePath.stripSuffix("/") + "/json",
      (request: HttpServletRequest) => page.renderJson(request), conf, basePath)
    attachHandler(renderHandler)
    attachHandler(renderJsonHandler)
    val handlers = pageToHandlers.getOrElseUpdate(page, ArrayBuffer[ServletContextHandler]())
    handlers += renderHandler
  }

  /** Attach a handler to this UI. */
  def attachHandler(handler: ServletContextHandler) {
    handlers += handler
    serverInfo.foreach(_.addHandler(handler))
  }

  /** Detach a handler from this UI. */
  def detachHandler(handler: ServletContextHandler) {
    handlers -= handler
    serverInfo.foreach(_.removeHandler(handler))
  }

  /**
    * Add a handler for static content.
    *
    * @param resourceBase Root of where to find resources to serve.
    * @param path         Path in UI where to mount the resources.
    */
  def addStaticHandler(resourceBase: String, path: String): Unit = {
    attachHandler(JettyUtils.createStaticHandler(resourceBase, path))
  }

  /**
    * Remove a static content handler.
    *
    * @param path Path in UI to unmount.
    */
  def removeStaticHandler(path: String): Unit = {
    handlers.find(_.getContextPath == path).foreach(detachHandler)
  }

  /** Initialize all components of the server. */
  def initialize(): Unit

  /** Bind to the HTTP server behind this web interface. */
  def bind(): Unit = {
    assert(serverInfo.isEmpty, s"Attempted to bind $className more than once!")
    try {
      val host = "0.0.0.0"
//      val host = InetAddress.getLocalHost.getHostAddress
      serverInfo = Some(startJettyServer(host, port, handlers, conf, name))
      logInfo(s"Bound $className to $host, and started at $webUrl")
    } catch {
      case e: Exception =>
        logError(s"Failed to bind $className", e)
        System.exit(1)
    }
  }

  /** Return the url of web interface. Only valid after bind(). */
  def webUrl: String = s"http://$publicHostName:$boundPort"

  /** Return the actual port to which this server is bound. Only valid after bind(). */
  def boundPort: Int = serverInfo.map(_.boundPort).getOrElse(-1)

  /** Stop the server behind this web interface. Only valid after bind(). */
  def stop(): Unit = {
    assert(serverInfo.isDefined,
      s"Attempted to stop $className before binding to a server!")
    serverInfo.get.stop()
  }
}


/**
  * A tab that represents a collection of pages.
  * The prefix is appended to the parent address to form a full path, and must not contain slashes.
  */
private[holder] abstract class WebUITab(parent: WebUI, val prefix: String) {
  val pages: ArrayBuffer[WebUIPage] = ArrayBuffer[WebUIPage]()
  val name: String = prefix.capitalize

  /** Attach a page to this tab. This prepends the page's prefix with the tab's own prefix. */
  def attachPage(page: WebUIPage) {
    page.prefix = (prefix + "/" + page.prefix).stripSuffix("/")
    pages += page
  }

  /** Get a list of header tabs from the parent UI. */
  def headerTabs: Seq[WebUITab] = parent.getTabs

  def basePath: String = parent.getBasePath
}


/**
  * A page that represents the leaf node in the UI hierarchy.
  *
  * The direct parent of a WebUIPage is not specified as it can be either a WebUI or a WebUITab.
  * If the parent is a WebUI, the prefix is appended to the parent's address to form a full path.
  * Else, if the parent is a WebUITab, the prefix is appended to the super prefix of the parent
  * to form a relative path. The prefix must not contain slashes.
  */
private[holder] abstract class WebUIPage(var prefix: String) {
  def render(request: HttpServletRequest): Seq[Node]

  def renderJson(request: HttpServletRequest): JValue = JNothing
}

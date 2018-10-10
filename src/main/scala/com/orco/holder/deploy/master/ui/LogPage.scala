package com.orco.holder.deploy.master.ui


import java.io.File
import javax.servlet.http.HttpServletRequest

import com.orco.holder.internal.Logging
import com.orco.holder.ui.{UIUtils, WebUIPage}
import com.orco.holder.util.Utils
import com.orco.holder.util.logging.RollingFileAppender

import scala.xml.{Node, Unparsed}

private[ui] class LogPage(parent: MasterWebUI) extends WebUIPage("logPage") with Logging {
  private val master = parent.master
  private val workDir = new File(parent.workDir.toURI.normalize().getPath)
  private val supportedLogTypes = Set("stderr", "stdout")
  private val defaultBytes = 100 * 1024

  // stripXSS is called first to remove suspicious characters used in XSS attacks
  def renderLog(request: HttpServletRequest): String = {
    val appId = Option(UIUtils.stripXSS(request.getParameter("appId")))
//    val executorId = Option(UIUtils.stripXSS(request.getParameter("executorId")))
//    val driverId = Option(UIUtils.stripXSS(request.getParameter("driverId")))
    val logType = UIUtils.stripXSS(request.getParameter("logType"))
    val offset = Option(UIUtils.stripXSS(request.getParameter("offset"))).map(_.toLong)
    val byteLength =
      Option(UIUtils.stripXSS(request.getParameter("byteLength"))).map(_.toInt)
        .getOrElse(defaultBytes)

//    val logDir = (appId, executorId, driverId) match {
    val logDir = appId match {
      case (Some(a)) =>
        s"${workDir.getPath}/$a/"
      case _ =>
        throw new Exception("Request must specify either application or driver identifiers")
    }

    val (logText, startByte, endByte, logLength) = getLog(logDir, logType, offset, byteLength)
    val pre = s"==== Bytes $startByte-$endByte of $logLength of $logDir$logType ====\n"
    pre + logText
  }

  // stripXSS is called first to remove suspicious characters used in XSS attacks
  def render(request: HttpServletRequest): Seq[Node] = {
    val appId = Option(UIUtils.stripXSS(request.getParameter("appId")))
//    val executorId = Option(UIUtils.stripXSS(request.getParameter("executorId")))
//    val driverId = Option(UIUtils.stripXSS(request.getParameter("driverId")))
    val logType = UIUtils.stripXSS(request.getParameter("logType"))
    val offset = Option(UIUtils.stripXSS(request.getParameter("offset"))).map(_.toLong)
    val byteLength =
      Option(UIUtils.stripXSS(request.getParameter("byteLength"))).map(_.toInt)
        .getOrElse(defaultBytes)

//    val (logDir, params, pageName) = (appId, executorId, driverId) match {
    val (logDir, params, pageName) = appId match {
      case (Some(a)) =>
        (s"${workDir.getPath}/$a/", s"appId=$a", s"$a")
      case _ =>
        throw new Exception("Request must specify either application or driver identifiers")
    }

    val (logText, startByte, endByte, logLength) = getLog(logDir, logType, offset, byteLength)
//    val linkToMaster = <p><a href={worker.activeMasterWebUiUrl}>Back to Master</a></p>
    val curLogLength = endByte - startByte
    val range =
      <span id="log-data">
        Showing {curLogLength} Bytes: {startByte.toString} - {endByte.toString} of {logLength}
      </span>

    val moreButton =
      <button type="button" onclick={"loadMore()"} class="log-more-btn btn btn-default">
        Load More
      </button>

    val newButton =
      <button type="button" onclick={"loadNew()"} class="log-new-btn btn btn-default">
        Load New
      </button>

    val alert =
      <div class="no-new-alert alert alert-info" style="display: none;">
        End of Log
      </div>

    val logParams = "?%s&logType=%s".format(params, logType)
    val jsOnload = "window.onload = " +
      s"initLogPage('$logParams', $curLogLength, $startByte, $endByte, $logLength, $byteLength);"

    val content =
      <div>
        {range}
        <div class="log-content" style="height:80vh; overflow:auto; padding:5px;">
          <div>{moreButton}</div>
          <pre>{logText}</pre>
          {alert}
          <div>{newButton}</div>
        </div>
        <script>{Unparsed(jsOnload)}</script>
      </div>

    UIUtils.basicSparkPage(content, logType + " log page for " + pageName)
  }

  /** Get the part of the log files given the offset and desired length of bytes */
  private def getLog(
                      logDirectory: String,
                      logType: String,
                      offsetOption: Option[Long],
                      byteLength: Int
                    ): (String, Long, Long, Long) = {

    if (!supportedLogTypes.contains(logType)) {
      return ("Error: Log type must be one of " + supportedLogTypes.mkString(", "), 0, 0, 0)
    }

    // Verify that the normalized path of the log directory is in the working directory
    val normalizedUri = new File(logDirectory).toURI.normalize()
    val normalizedLogDir = new File(normalizedUri.getPath)
    if (!Utils.isInDirectory(workDir, normalizedLogDir)) {
      return ("Error: invalid log directory " + logDirectory, 0, 0, 0)
    }

    try {
      val files = RollingFileAppender.getSortedRolledOverFiles(logDirectory, logType)
      logDebug(s"Sorted log files of type $logType in $logDirectory:\n${files.mkString("\n")}")

      val fileLengths: Seq[Long] = files.map(Utils.getFileLength(_, master.conf))
      val totalLength = fileLengths.sum
      val offset = offsetOption.getOrElse(totalLength - byteLength)
      val startIndex = {
        if (offset < 0) {
          0L
        } else if (offset > totalLength) {
          totalLength
        } else {
          offset
        }
      }
      val endIndex = math.min(startIndex + byteLength, totalLength)
      logDebug(s"Getting log from $startIndex to $endIndex")
      val logText = Utils.offsetBytes(files, fileLengths, startIndex, endIndex)
      logDebug(s"Got log of length ${logText.length} bytes")
      (logText, startIndex, endIndex, totalLength)
    } catch {
      case e: Exception =>
        logError(s"Error getting $logType logs from directory $logDirectory", e)
        ("Error getting logs due to exception: " + e.getMessage, 0, 0, 0)
    }
  }
}


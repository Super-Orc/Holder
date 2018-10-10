package com.orco.holder.ui

import java.text.SimpleDateFormat
import java.util.{Date, Locale}

import com.orco.holder.internal.Logging
import org.apache.commons.lang3.StringEscapeUtils

import scala.xml.{Node, Text}

private[holder] object UIUtils extends Logging {
  private val NEWLINE_AND_SINGLE_QUOTE_REGEX = raw"(?i)(\r\n|\n|\r|%0D%0A|%0A|%0D|'|%27)".r

  val TABLE_CLASS_NOT_STRIPED = "table table-bordered table-condensed"
  val TABLE_CLASS_STRIPED: String = TABLE_CLASS_NOT_STRIPED + " table-striped"
  val TABLE_CLASS_STRIPED_SORTABLE: String = TABLE_CLASS_STRIPED + " sortable"

  // SimpleDateFormat is not thread-safe. Don't expose it to avoid improper use.
  private val dateFormat = new ThreadLocal[SimpleDateFormat]() {
    override def initialValue(): SimpleDateFormat =
      new SimpleDateFormat("yyyy/MM/dd HH:mm:ss", Locale.US)
  }

  def formatDate(date: Date): String = dateFormat.get.format(date)

  def formatDuration(milliseconds: Long): String = {
    if (milliseconds < 100) {
      return "%d ms".format(milliseconds)
    }
    val seconds = milliseconds.toDouble / 1000
    if (seconds < 1) {
      return "%.1f s".format(seconds)
    }
    if (seconds < 60) {
      return "%.0f s".format(seconds)
    }
    val minutes = seconds / 60
    if (minutes < 10) {
      return "%.1f min".format(minutes)
    } else if (minutes < 60) {
      return "%.0f min".format(minutes)
    }
    val hours = minutes / 60
    "%.1f h".format(hours)
  }

  /** Returns an HTML table constructed by generating a row for each object in a sequence. */
  def listingTable[T](
                       headers: Seq[String],
                       generateDataRow: T => Seq[Node],
                       data: Iterable[T],
                       fixedWidth: Boolean = false,
                       id: Option[String] = None,
                       headerClasses: Seq[String] = Seq.empty,
                       stripeRowsWithCss: Boolean = true,
                       sortable: Boolean = true): Seq[Node] = {

    val listingTableClass = {
      val _tableClass = if (stripeRowsWithCss) TABLE_CLASS_STRIPED else TABLE_CLASS_NOT_STRIPED
      if (sortable) {
        _tableClass + " sortable"
      } else {
        _tableClass
      }
    }
    val colWidth = 100.toDouble / headers.size
    val colWidthAttr = if (fixedWidth) colWidth + "%" else ""

    def getClass(index: Int): String = {
      if (index < headerClasses.size) {
        headerClasses(index)
      } else {
        ""
      }
    }

    val newlinesInHeader = headers.exists(_.contains("\n"))

    def getHeaderContent(header: String): Seq[Node] = {
      if (newlinesInHeader) {
        <ul class="unstyled">
          {header.split("\n").map { case t => <li>
          {t}
        </li>
        }}
        </ul>
      } else {
        Text(header)
      }
    }

    val headerRow: Seq[Node] = {
      headers.view.zipWithIndex.map { x =>
        <th width={colWidthAttr} class={getClass(x._2)}>
          {getHeaderContent(x._1)}
        </th>
      }
    }
    <table class={listingTableClass} id={id.map(Text.apply)}>
      <thead>
        {headerRow}
      </thead>
      <tbody>
        {data.map(r => generateDataRow(r))}
      </tbody>
    </table>
  }


  /** Returns a page with the holder css/js and a simple format. Used for scheduler UI. */
  def basicSparkPage(
                      content: => Seq[Node],
                      title: String,
                      useDataTables: Boolean = false): Seq[Node] = {
    <html>
      <head>
        {commonHeaderNodes}{if (useDataTables) dataTablesHeaderNodes else Seq.empty}<title>
        {title}
      </title>
      </head>
      <body>
        <div class="container-fluid">
          <div class="row-fluid">
            <div class="span12">
              <h3 style="vertical-align: middle; display: inline-block;">
                <a style="text-decoration: none" href={prependBaseUri("/")}>
                  <img src={prependBaseUri("/static/holder-logo-77x50px-hd.png")}/>
                </a>{title}
              </h3>
            </div>
          </div>{content}
        </div>
      </body>
    </html>
  }

  def uiRoot: String = {
    sys.props.get("holder.ui.proxyBase")
      .orElse(sys.env.get("APPLICATION_WEB_PROXY_BASE"))
      .getOrElse("")
  }

  def prependBaseUri(basePath: String = "", resource: String = ""): String = {
    uiRoot + basePath + resource
  }

  def commonHeaderNodes: Seq[Node] = {
      <meta http-equiv="Content-type" content="text/html; charset=utf-8"/>
        <link rel="stylesheet" href={prependBaseUri("/static/bootstrap.min.css")} type="text/css"/>
        <link rel="stylesheet" href={prependBaseUri("/static/vis.min.css")} type="text/css"/>
        <link rel="stylesheet" href={prependBaseUri("/static/webui.css")} type="text/css"/>
        <link rel="stylesheet" href={prependBaseUri("/static/timeline-view.css")} type="text/css"/>
      <script src={prependBaseUri("/static/sorttable.js")}></script>
      <script src={prependBaseUri("/static/jquery-1.11.1.min.js")}></script>
      <script src={prependBaseUri("/static/vis.min.js")}></script>
      <script src={prependBaseUri("/static/bootstrap-tooltip.js")}></script>
      <script src={prependBaseUri("/static/initialize-tooltips.js")}></script>
      <script src={prependBaseUri("/static/table.js")}></script>
      <script src={prependBaseUri("/static/additional-metrics.js")}></script>
      <script src={prependBaseUri("/static/timeline-view.js")}></script>
      <script src={prependBaseUri("/static/log-view.js")}></script>
      <script src={prependBaseUri("/static/webui.js")}></script>
      <script>setUIRoot('
        {UIUtils.uiRoot}
        ')</script>
  }

  def dataTablesHeaderNodes: Seq[Node] = {
      <link rel="stylesheet"
            href={prependBaseUri("/static/jquery.dataTables.1.10.4.min.css")} type="text/css"/>
        <link rel="stylesheet"
              href={prependBaseUri("/static/dataTables.bootstrap.css")} type="text/css"/>
        <link rel="stylesheet" href={prependBaseUri("/static/jsonFormatter.min.css")} type="text/css"/>
      <script src={prependBaseUri("/static/jquery.dataTables.1.10.4.min.js")}></script>
      <script src={prependBaseUri("/static/jquery.cookies.2.2.0.min.js")}></script>
      <script src={prependBaseUri("/static/jquery.blockUI.min.js")}></script>
      <script src={prependBaseUri("/static/dataTables.bootstrap.min.js")}></script>
      <script src={prependBaseUri("/static/jsonFormatter.min.js")}></script>
      <script src={prependBaseUri("/static/jquery.mustache.js")}></script>
  }

  /**
    * Remove suspicious characters of user input to prevent Cross-Site scripting (XSS) attacks
    *
    * For more information about XSS testing:
    * https://www.owasp.org/index.php/XSS_Filter_Evasion_Cheat_Sheet and
    * https://www.owasp.org/index.php/Testing_for_Reflected_Cross_site_scripting_(OTG-INPVAL-001)
    */
  def stripXSS(requestParameter: String): String = {
    if (requestParameter == null) {
      null
    } else {
      // Remove new lines and single quotes, followed by escaping HTML version 4.0
      StringEscapeUtils.escapeHtml4(
        NEWLINE_AND_SINGLE_QUOTE_REGEX.replaceAllIn(requestParameter, ""))
    }
  }

}

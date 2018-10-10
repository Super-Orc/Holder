package com.orco.holder.util

import java.io._
import java.lang.management.ManagementFactory
import java.math.{MathContext, RoundingMode}
import java.net._
import java.nio.channels.FileChannel
import java.nio.file.{Files, Paths}
import java.util.Locale
import java.util.concurrent.TimeUnit
import java.util.zip.GZIPInputStream

import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import com.google.common.io.ByteStreams
import com.orco.holder.{HolderConf, HolderException}
import com.orco.holder.internal.Logging
import com.orco.holder.network.util.JavaUtils
import io.netty.channel.unix.Errors.NativeIoException
import org.apache.commons.lang3.SystemUtils
import org.slf4j.Logger
import scala.collection.JavaConversions._
import scala.annotation.tailrec
import scala.io.Source
import scala.util.control.{ControlThrowable, NonFatal}
/**
  * Various utility methods used by holder.
  */
private[holder] object Utils extends Logging {

  private val sparkUncaughtExceptionHandler = new HolderUncaughtExceptionHandler


  /**
    * Whether the underlying operating system is Windows.
    */
  val isWindows = SystemUtils.IS_OS_WINDOWS

  /**
    * Whether the underlying operating system is Mac OS X.
    */
  val isMac = SystemUtils.IS_OS_MAC_OSX

  /**
    * Pattern for matching a Windows drive, which contains only a single alphabet character.
    */
  val windowsDrive = "([a-zA-Z])".r

  /**
    * Return the current system LD_LIBRARY_PATH name
    */
  def libraryPathEnvName: String = {
    if (isWindows) {
      "PATH"
    } else if (isMac) {
      "DYLD_LIBRARY_PATH"
    } else {
      "LD_LIBRARY_PATH"
    }
  }
  /**
    * Get the local machine's FQDN.
    */
  def localCanonicalHostName(): String = {
    localIpAddress.getCanonicalHostName
  }
  /**
    * Get the local host's IP address in dotted-quad format (e.g. 1.2.3.4).
    * Note, this is typically not used from within core holder.
    */
  private lazy val localIpAddress: InetAddress = findLocalInetAddress()

  private def findLocalInetAddress(): InetAddress = {
    val defaultIpOverride = System.getenv("SPARK_LOCAL_IP")
    if (defaultIpOverride != null) {
      InetAddress.getByName(defaultIpOverride)
    } else {
      val address = InetAddress.getLocalHost
      if (address.isLoopbackAddress) {
        // Address resolves to something like 127.0.1.1, which happens on Debian; try to find
        // a better address using the local network interfaces
        // getNetworkInterfaces returns ifs in reverse order compared to ifconfig output order
        // on unix-like system. On windows, it returns in index order.
        // It's more proper to pick ip address following system output order.
        val activeNetworkIFs = NetworkInterface.getNetworkInterfaces.toSeq
        val reOrderedNetworkIFs = if (isWindows) activeNetworkIFs else activeNetworkIFs.reverse

        for (ni <- reOrderedNetworkIFs) {
          val addresses = ni.getInetAddresses
            .filterNot(addr => addr.isLinkLocalAddress || addr.isLoopbackAddress).toSeq
          if (addresses.nonEmpty) {
            val addr = addresses.find(_.isInstanceOf[Inet4Address]).getOrElse(addresses.head)
            // because of Inet6Address.toHostName may add interface at the end if it knows about it
            val strippedAddress = InetAddress.getByAddress(addr.getAddress)
            // We've found an address that looks reasonable!
            logWarning("Your hostname, " + InetAddress.getLocalHost.getHostName + " resolves to" +
              " a loopback address: " + address.getHostAddress + "; using " +
              strippedAddress.getHostAddress + " instead (on interface " + ni.getName + ")")
            logWarning("Set SPARK_LOCAL_IP if you need to bind to another address")
            return strippedAddress
          }
        }
        logWarning("Your hostname, " + InetAddress.getLocalHost.getHostName + " resolves to" +
          " a loopback address: " + address.getHostAddress + ", but we couldn't find any" +
          " external IP address!")
        logWarning("Set SPARK_LOCAL_IP if you need to bind to another address")
      }
      address
    }
  }

  /**
    * Return the file length, if the file is compressed it returns the uncompressed file length.
    * It also caches the uncompressed file size to avoid repeated decompression. The cache size is
    * read from workerConf.
    */
  def getFileLength(file: File, workConf: HolderConf): Long = {
    if (file.getName.endsWith(".gz")) {
      getCompressedLogFileLengthCache(workConf).get(file.getAbsolutePath)
    } else {
      file.length
    }
  }
  private val UNCOMPRESSED_LOG_FILE_LENGTH_CACHE_SIZE_CONF =
    "holder.worker.ui.compressedLogFileLengthCacheSize"
  private val DEFAULT_UNCOMPRESSED_LOG_FILE_LENGTH_CACHE_SIZE = 100
  private var compressedLogFileLengthCache: LoadingCache[String, java.lang.Long] = _

  private def getCompressedLogFileLengthCache(
                                               sparkConf: HolderConf): LoadingCache[String, java.lang.Long] = this.synchronized {
    if (compressedLogFileLengthCache == null) {
      val compressedLogFileLengthCacheSize = sparkConf.getInt(
        UNCOMPRESSED_LOG_FILE_LENGTH_CACHE_SIZE_CONF,
        DEFAULT_UNCOMPRESSED_LOG_FILE_LENGTH_CACHE_SIZE)
      compressedLogFileLengthCache = CacheBuilder.newBuilder()
        .maximumSize(compressedLogFileLengthCacheSize)
        .build[String, java.lang.Long](new CacheLoader[String, java.lang.Long]() {
        override def load(path: String): java.lang.Long = {
          Utils.getCompressedFileLength(new File(path))
        }
      })
    }
    compressedLogFileLengthCache
  }
  /** Return uncompressed file length of a compressed file. */
  private def getCompressedFileLength(file: File): Long = {
    var gzInputStream: GZIPInputStream = null
    try {
      // Uncompress .gz file to determine file size.
      var fileSize = 0L
      gzInputStream = new GZIPInputStream(new FileInputStream(file))
      val bufSize = 1024
      val buf = new Array[Byte](bufSize)
      var numBytes = ByteStreams.read(gzInputStream, buf, 0, bufSize)
      while (numBytes > 0) {
        fileSize += numBytes
        numBytes = ByteStreams.read(gzInputStream, buf, 0, bufSize)
      }
      fileSize
    } catch {
      case e: Throwable =>
        logError(s"Cannot get file length of $file", e)
        throw e
    } finally {
      if (gzInputStream != null) {
        gzInputStream.close()
      }
    }
  }
  // scalastyle:off classforname
  /** Preferred alternative to Class.forName(className) */
  def classForName(className: String): Class[_] = {
    Class.forName(className, true, getContextOrSparkClassLoader)
    // scalastyle:on classforname
  }

  /**
    * Convert a quantity in megabytes to a human-readable string such as "4.0 MB".
    */
  def megabytesToString(megabytes: Long): String = {
    bytesToString(megabytes * 1024L * 1024L)
  }

  /**
    * Return whether the specified file is a parent directory of the child file.
    */
  @tailrec
  def isInDirectory(parent: File, child: File): Boolean = {
    if (child == null || parent == null) {
      return false
    }
    if (!child.exists() || !parent.exists() || !parent.isDirectory) {
      return false
    }
    if (parent.equals(child)) {
      return true
    }
    isInDirectory(parent, child.getParentFile)
  }

  /**
    * Return a string containing data across a set of files. The `startIndex`
    * and `endIndex` is based on the cumulative size of all the files take in
    * the given order. See figure below for more details.
    */
  def offsetBytes(files: Seq[File], fileLengths: Seq[Long], start: Long, end: Long): String = {
    assert(files.length == fileLengths.length)
    val startIndex = math.max(start, 0)
    val endIndex = math.min(end, fileLengths.sum)
    val fileToLength = files.zip(fileLengths).toMap
    logDebug("Log files: \n" + fileToLength.mkString("\n"))

    val stringBuffer = new StringBuffer((endIndex - startIndex).toInt)
    var sum = 0L
    files.zip(fileLengths).foreach { case (file, fileLength) =>
      val startIndexOfFile = sum
      val endIndexOfFile = sum + fileToLength(file)
      logDebug(s"Processing file $file, " +
        s"with start index = $startIndexOfFile, end index = $endIndex")

      /*
                                      ____________
       range 1:                      |            |
                                     |   case A   |

       files:   |==== file 1 ====|====== file 2 ======|===== file 3 =====|

                     |   case B  .       case C       .    case D    |
       range 2:      |___________.____________________.______________|
       */

      if (startIndex <= startIndexOfFile  && endIndex >= endIndexOfFile) {
        // Case C: read the whole file
        stringBuffer.append(offsetBytes(file.getAbsolutePath, fileLength, 0, fileToLength(file)))
      } else if (startIndex > startIndexOfFile && startIndex < endIndexOfFile) {
        // Case A and B: read from [start of required range] to [end of file / end of range]
        val effectiveStartIndex = startIndex - startIndexOfFile
        val effectiveEndIndex = math.min(endIndex - startIndexOfFile, fileToLength(file))
        stringBuffer.append(Utils.offsetBytes(
          file.getAbsolutePath, fileLength, effectiveStartIndex, effectiveEndIndex))
      } else if (endIndex > startIndexOfFile && endIndex < endIndexOfFile) {
        // Case D: read from [start of file] to [end of require range]
        val effectiveStartIndex = math.max(startIndex - startIndexOfFile, 0)
        val effectiveEndIndex = endIndex - startIndexOfFile
        stringBuffer.append(Utils.offsetBytes(
          file.getAbsolutePath, fileLength, effectiveStartIndex, effectiveEndIndex))
      }
      sum += fileToLength(file)
      logDebug(s"After processing file $file, string built is ${stringBuffer.toString}")
    }
    stringBuffer.toString
  }

  /** Return a string containing part of a file from byte 'start' to 'end'. */
  def offsetBytes(path: String, length: Long, start: Long, end: Long): String = {
    val file = new File(path)
    val effectiveEnd = math.min(length, end)
    val effectiveStart = math.max(0, start)
    val buff = new Array[Byte]((effectiveEnd-effectiveStart).toInt)
    val stream = if (path.endsWith(".gz")) {
      new GZIPInputStream(new FileInputStream(file))
    } else {
      new FileInputStream(file)
    }

    try {
      ByteStreams.skipFully(stream, effectiveStart)
      ByteStreams.readFully(stream, buff)
    } finally {
      stream.close()
    }
    Source.fromBytes(buff).mkString
  }

  /**
    * Execute a block of code that evaluates to Unit, forwarding any uncaught exceptions to the
    * default UncaughtExceptionHandler
    *
    * NOTE: This method is to be called by the holder-started JVM process.
    */
  def tryOrExit(block: => Unit) {
    try {
      block
    } catch {
      case e: ControlThrowable => throw e
      case t: Throwable => sparkUncaughtExceptionHandler.uncaughtException(t)
    }
  }

  /** Executes the given block. Log non-fatal errors if any, and only throw fatal errors */
  def tryLogNonFatalError(block: => Unit) {
    try {
      block
    } catch {
      case NonFatal(t) =>
        logError(s"Uncaught exception in thread ${Thread.currentThread().getName}", t)
    }
  }

  /** Return the class name of the given object, removing all dollar signs */
  def getFormattedClassName(obj: AnyRef): String = {
    obj.getClass.getSimpleName.replace("$", "")
  }

  /**
    * Convert a quantity in bytes to a human-readable string such as "4.0 MB".
    */
  def bytesToString(size: Long): String = bytesToString(BigInt(size))

  def bytesToString(size: BigInt): String = {
    val EB = 1L << 60
    val PB = 1L << 50
    val TB = 1L << 40
    val GB = 1L << 30
    val MB = 1L << 20
    val KB = 1L << 10

    if (size >= BigInt(1L << 11) * EB) {
      // The number is too large, show it in scientific notation.
      BigDecimal(size, new MathContext(3, RoundingMode.HALF_UP)).toString() + " B"
    } else {
      val (value, unit) = {
        if (size >= 2 * EB) {
          (BigDecimal(size) / EB, "EB")
        } else if (size >= 2 * PB) {
          (BigDecimal(size) / PB, "PB")
        } else if (size >= 2 * TB) {
          (BigDecimal(size) / TB, "TB")
        } else if (size >= 2 * GB) {
          (BigDecimal(size) / GB, "GB")
        } else if (size >= 2 * MB) {
          (BigDecimal(size) / MB, "MB")
        } else if (size >= 2 * KB) {
          (BigDecimal(size) / KB, "KB")
        } else {
          (BigDecimal(size), "B")
        }
      }
      "%.1f %s".formatLocal(Locale.US, value, unit)
    }
  }

  private def listFilesSafely(file: File): Seq[File] = {
    if (file.exists()) {
      val files = file.listFiles()
      if (files == null) {
        throw new IOException("Failed to list files for dir: " + file)
      }
      files
    } else {
      List()
    }
  }

  /**
    * Check to see if file is a symbolic link.
    */
  def isSymlink(file: File): Boolean = {
    return Files.isSymbolicLink(Paths.get(file.toURI))
  }

  /**
    * Execute a block of code, then a finally block, but if exceptions happen in
    * the finally block, do not suppress the original exception.
    *
    * This is primarily an issue with `finally { out.close() }` blocks, where
    * close needs to be called to clean up `out`, but if an exception happened
    * in `out.write`, it's likely `out` may be corrupted and `out.close` will
    * fail as well. This would then suppress the original/likely more meaningful
    * exception from the original `out.write` call.
    */
  def tryWithSafeFinally[T](block: => T)(finallyBlock: => Unit): T = {
    var originalThrowable: Throwable = null
    try {
      block
    } catch {
      case t: Throwable =>
        // Purposefully not using NonFatal, because even fatal exceptions
        // we don't want to have our finallyBlock suppress
        originalThrowable = t
        throw originalThrowable
    } finally {
      try {
        finallyBlock
      } catch {
        case t: Throwable if (originalThrowable != null && originalThrowable != t) =>
          originalThrowable.addSuppressed(t)
          logWarning(s"Suppressing exception in finally: ${t.getMessage}", t)
          throw originalThrowable
      }
    }
  }

  /**
    * Execute the given block, logging and re-throwing any uncaught exception.
    * This is particularly useful for wrapping code that runs in a thread, to ensure
    * that exceptions are printed, and to avoid having to catch Throwable.
    */
  def logUncaughtExceptions[T](f: => T): T = {
    try {
      f
    } catch {
      case ct: ControlThrowable =>
        throw ct
      case t: Throwable =>
        logError(s"Uncaught exception in thread ${Thread.currentThread().getName}", t)
        throw t
    }
  }

  def checkHost(host: String) {
    assert(host != null && host.indexOf(':') == -1, s"Expected hostname (not IP) but got $host")
  }

  def checkHostPort(hostPort: String) {
    assert(hostPort != null && hostPort.indexOf(':') != -1,
      s"Expected host and port but got $hostPort")
  }

  /**
    * Get the Context ClassLoader on this thread or, if not present, the ClassLoader that
    * loaded holder.
    *
    * This should be used whenever passing a ClassLoader to Class.ForName or finding the currently
    * active loader when setting up ClassLoader delegation chains.
    */
  def getContextOrSparkClassLoader: ClassLoader =
    Option(Thread.currentThread().getContextClassLoader).getOrElse(getSparkClassLoader)

  /**
    * Get the ClassLoader which loaded holder.
    */
  def getSparkClassLoader: ClassLoader = getClass.getClassLoader

  /**
    * Utility function that should be called early in `main()` for daemons to set up some common
    * diagnostic state.
    */
  def initDaemon(log: Logger): Unit = {
    log.info(s"Started daemon with process name: ${Utils.getProcessName}")
    SignalUtils.registerLogger(log)
  }

  /**
    * Returns the name of this JVM process. This is OS dependent but typically (OSX, Linux, Windows),
    * this is formatted as PID@hostname.
    */
  def getProcessName: String = {
    ManagementFactory.getRuntimeMXBean.getName
  }

  /**
    * Convert a time parameter such as (50s, 100ms, or 250us) to microseconds for internal use. If
    * no suffix is provided, the passed number is assumed to be in ms.
    */
  def timeStringAsMs(str: String): Long = {
    JavaUtils.timeStringAsMs(str)
  }

  /**
    * Attempt to start a service on the given port, or fail after a number of attempts.
    * Each subsequent attempt uses 1 + the port used in the previous attempt (unless the port is 0).
    *
    * @param startPort    The initial port to start the service on.
    * @param startService Function to start service on a given port.
    *                     This is expected to throw java.net.BindException on port collision.
    * @param conf         A HolderConf used to get the maximum number of retries when binding to a port.
    * @param serviceName  Name of the service.
    * @return (service: T, port: Int)
    */
  def startServiceOnPort[T](
                             startPort: Int,
                             startService: Int => (T, Int),
                             conf: HolderConf,
                             serviceName: String = ""): (T, Int) = {

    require(startPort == 0 || (1024 <= startPort && startPort < 65536),
      "startPort should be between 1024 and 65535 (inclusive), or 0 for a random free port.")

    val serviceString = if (serviceName.isEmpty) "" else s" '$serviceName'"
    val maxRetries = portMaxRetries(conf)
    for (offset <- 0 to maxRetries) {
      // Do not increment port if startPort is 0, which is treated as a special port
      val tryPort = if (startPort == 0) {
        startPort
      } else {
        userPort(startPort, offset)
      }
      try {
        val (service, port) = startService(tryPort)
        logInfo(s"Successfully started service$serviceString on port $port.")
        return (service, port)
      } catch {
        case e: Exception if isBindCollision(e) =>
          if (offset >= maxRetries) {
            val exceptionMessage = if (startPort == 0) {
              s"${e.getMessage}: Service$serviceString failed after " +
                s"$maxRetries retries (on a random free port)! " +
                s"Consider explicitly setting the appropriate binding address for " +
                s"the service$serviceString (for example holder.driver.bindAddress " +
                s"for SparkDriver) to the correct binding address."
            } else {
              s"${e.getMessage}: Service$serviceString failed after " +
                s"$maxRetries retries (starting from $startPort)! Consider explicitly setting " +
                s"the appropriate port for the service$serviceString (for example holder.ui.port " +
                s"for SparkUI) to an available port or increasing holder.port.maxRetries."
            }
            val exception = new BindException(exceptionMessage)
            // restore original stack trace
            exception.setStackTrace(e.getStackTrace)
            throw exception
          }
          if (startPort == 0) {
            // As startPort 0 is for a random free port, it is most possibly binding address is
            // not correct.
            logWarning(s"Service$serviceString could not bind on a random free port. " +
              "You may check whether configuring an appropriate binding address.")
          } else {
            logWarning(s"Service$serviceString could not bind on port $tryPort. " +
              s"Attempting port ${tryPort + 1}.")
          }
      }
    }
    // Should never happen
    throw new HolderException(s"Failed to start service$serviceString on port $startPort")
  }

  /**
    * Return whether the exception is caused by an address-port collision when binding.
    */
  def isBindCollision(exception: Throwable): Boolean = {
    exception match {
      case e: BindException =>
        if (e.getMessage != null) {
          return true
        }
        isBindCollision(e.getCause)
      //      case e: MultiException =>
      //        e.getThrowables.asScala.exists(isBindCollision)
      case e: NativeIoException =>
        (e.getMessage != null && e.getMessage.startsWith("bind() failed: ")) ||
          isBindCollision(e.getCause)
      case e: Exception => isBindCollision(e.getCause)
      case _ => false
    }
  }

  /**
    * Returns the user port to try when trying to bind a service. Handles wrapping and skipping
    * privileged ports.
    */
  def userPort(base: Int, offset: Int): Int = {
    (base + offset - 1024) % (65536 - 1024) + 1024
  }


  /**
    * Maximum number of retries when binding to a port before giving up.
    */
  def portMaxRetries(conf: HolderConf): Int = {
    val maxRetries = conf.getOption("holder.port.maxRetries").map(_.toInt)
    if (conf.contains("holder.testing")) {
      // Set a higher number of retries for tests...
      maxRetries.getOrElse(100)
    } else {
      maxRetries.getOrElse(16)
    }
  }

  /**
    * A file name may contain some invalid URI characters, such as " ". This method will convert the
    * file name to a raw path accepted by `java.net.URI(String)`.
    *
    * Note: the file name must not contain "/" or "\"
    */
  def encodeFileNameToURIRawPath(fileName: String): String = {
    require(!fileName.contains("/") && !fileName.contains("\\"))
    // `file` and `localhost` are not used. Just to prevent URI from parsing `fileName` as
    // scheme or host. The prefix "/" is required because URI doesn't accept a relative path.
    // We should remove it after we get the raw path.
    new URI("file", null, "localhost", -1, "/" + fileName, null, null).getRawPath.substring(1)
  }

  /**
    * Return a pair of host and port extracted from the `sparkUrl`.
    *
    * A holder url (`holder://host:port`) is a special URI that its scheme is `holder` and only contains
    * host and port.
    *
    *  org.apache.holder.HolderException if sparkUrl is invalid.
    */
  @throws(classOf[HolderException])
  def extractHostPortFromSparkUrl(sparkUrl: String): (String, Int) = {
    try {
      val uri = new java.net.URI(sparkUrl)
      val host = uri.getHost
      val port = uri.getPort
      if (uri.getScheme != "holder" ||
        host == null ||
        port < 0 ||
        (uri.getPath != null && !uri.getPath.isEmpty) || // uri.getPath returns "" instead of null
        uri.getFragment != null ||
        uri.getQuery != null ||
        uri.getUserInfo != null) {
        throw new HolderException("Invalid master URL: " + sparkUrl)
      }
      (host, port)
    } catch {
      case e: java.net.URISyntaxException =>
        throw new HolderException("Invalid master URL: " + sparkUrl, e)
    }
  }

  /**
    * Execute a block of code that returns a value, re-throwing any non-fatal uncaught
    * exceptions as IOException. This is used when implementing Externalizable and Serializable's
    * read and write methods, since Java's serializer will not report non-IOExceptions properly;
    * see holder-4080 for more context.
    */
  def tryOrIOException[T](block: => T): T = {
    try {
      block
    } catch {
      case e: IOException =>
        logError("Exception encountered", e)
        throw e
      case NonFatal(e) =>
        logError("Exception encountered", e)
        throw new IOException(e)
    }
  }

  /**
    * Convert a time parameter such as (50s, 100ms, or 250us) to seconds for internal use. If
    * no suffix is provided, the passed number is assumed to be in seconds.
    */
  def timeStringAsSeconds(str: String): Long = {
    JavaUtils.timeStringAsSec(str)
  }


  /**
    * Delete a file or directory and its contents recursively.
    * Don't follow directories if they are symlinks.
    * Throws an exception if deletion is unsuccessful.
    */
  def deleteRecursively(file: File) {
    if (file != null) {
      try {
        if (file.isDirectory && !isSymlink(file)) {
          var savedIOException: IOException = null
          for (child <- listFilesSafely(file)) {
            try {
              deleteRecursively(child)
            } catch {
              // In case of multiple exceptions, only last one will be thrown
              case ioe: IOException => savedIOException = ioe
            }
          }
          if (savedIOException != null) {
            throw savedIOException
          }
          ShutdownHookManager.removeShutdownDeleteDir(file)
        }
      } finally {
        if (file.delete()) {
          logTrace(s"${file.getAbsolutePath} has been deleted")
        } else {
          // Delete can also fail if the file simply did not exist
          if (file.exists()) {
            throw new IOException("Failed to delete: " + file.getAbsolutePath)
          }
        }
      }
    }
  }

  /**
    * Terminates a process waiting for at most the specified duration.
    *
    * @return the process exit value if it was successfully terminated, else None
    */
  def terminateProcess(process: Process, timeoutMs: Long): Option[Int] = {
    // Politely destroy first
    process.destroy()
    if (process.waitFor(timeoutMs, TimeUnit.MILLISECONDS)) {
      // Successful exit
      Option(process.exitValue())
    } else {
      try {
        process.destroyForcibly()
      } catch {
        case NonFatal(e) => logWarning("Exception when attempting to kill process", e)
      }
      // Wait, again, although this really should return almost immediately
      if (process.waitFor(timeoutMs, TimeUnit.MILLISECONDS)) {
        Option(process.exitValue())
      } else {
        logWarning("Timed out waiting to forcibly kill process")
        None
      }
    }
  }

  /**
    * Copy all data from an InputStream to an OutputStream. NIO way of file stream to file stream
    * copying is disabled by default unless explicitly set transferToEnabled as true,
    * the parameter transferToEnabled should be configured by holder.file.transferTo = [true|false].
    */
  def copyStream(
                  in: InputStream,
                  out: OutputStream,
                  closeStreams: Boolean = false,
                  transferToEnabled: Boolean = false): Long = {
    tryWithSafeFinally {
      if (in.isInstanceOf[FileInputStream] && out.isInstanceOf[FileOutputStream]
        && transferToEnabled) {
        // When both streams are File stream, use transferTo to improve copy performance.
        val inChannel = in.asInstanceOf[FileInputStream].getChannel()
        val outChannel = out.asInstanceOf[FileOutputStream].getChannel()
        val size = inChannel.size()
        copyFileStreamNIO(inChannel, outChannel, 0, size)
        size
      } else {
        var count = 0L
        val buf = new Array[Byte](8192)
        var n = 0
        while (n != -1) {
          n = in.read(buf)
          if (n != -1) {
            out.write(buf, 0, n)
            count += n
          }
        }
        count
      }
    } {
      if (closeStreams) {
        try {
          in.close()
        } finally {
          out.close()
        }
      }
    }
  }

  def copyFileStreamNIO(
                         input: FileChannel,
                         output: FileChannel,
                         startPosition: Long,
                         bytesToCopy: Long): Unit = {
    val initialPos = output.position()
    var count = 0L
    // In case transferTo method transferred less data than we have required.
    while (count < bytesToCopy) {
      count += input.transferTo(count + startPosition, bytesToCopy - count, output)
    }
    assert(count == bytesToCopy,
      s"request to copy $bytesToCopy bytes, but actually copied $count bytes.")

    // Check the position after transferTo loop to see if it is in the right position and
    // give user information if not.
    // Position will not be increased to the expected length after calling transferTo in
    // kernel version 2.6.32, this issue can be seen in
    // https://bugs.openjdk.java.net/browse/JDK-7052359
    // This will lead to stream corruption issue when using sort-based shuffle (holder-3948).
    val finalPos = output.position()
    val expectedPos = initialPos + bytesToCopy
    assert(finalPos == expectedPos,
      s"""
         |Current position $finalPos do not equal to expected position $expectedPos
         |after transferTo, please check your kernel version to see if it is 2.6.32,
         |this is a kernel bug which will lead to unexpected behavior when using transferTo.
         |You can set holder.file.transferTo = false to disable this NIO feature.
           """.stripMargin)
  }
}

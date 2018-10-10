package com.orco.holder.deploy.worker

import java.io._
import java.nio.charset.StandardCharsets

import com.google.common.io.Files
import com.orco.holder.HolderConf
import com.orco.holder.deploy.{Command, ExecutorState}
import com.orco.holder.internal.Logging
import com.orco.holder.rpc.RpcEndpointRef
import com.orco.holder.scheduler.cluster.CoarseGrainedClusterMessages.ExecutorRemove
import com.orco.holder.util.{ShutdownHookManager, Utils}
import com.orco.holder.util.logging.FileAppender

import scala.collection.JavaConverters._

/**
  * Manages the execution of one executor process.
  * This is currently only used in standalone mode.
  */
private[orco] class ExecutorRunner(
                                    val name: String,
                                    val _command: Command,
                                    //                                    val cores: Int,
                                    val memory: Int,
                                    var master: RpcEndpointRef,
                                    val sparkHome: File,
                                    val executorDir: File,
                                    conf: HolderConf,
                                    @volatile var state: ExecutorState.Value)
  extends Logging with Serializable {

  //  private val fullId = appId + "/" + execId
  @transient var valid: Integer = 1
  //null 经过网络传输
  @transient private var workerThread: Thread = _
  @transient private var process: Process = _
  @transient private var stdoutAppender: FileAppender = _
  @transient private var stderrAppender: FileAppender = _

  // Timeout to wait for when trying to terminate an executor.
  private val EXECUTOR_TERMINATE_TIMEOUT_MS = 10 * 1000

  // NOTE: This is now redundant with the automated shut-down enforced by the Executor. It might
  // make sense to remove this in the future.
  @transient private var shutdownHook: AnyRef = _

  private[orco] def start() {
    workerThread = new Thread("ExecutorRunner for " + name) {
      override def run() {
        fetchAndRunExecutor()
      }
    }
    workerThread.start()
    // Shutdown hook that kills actors on shutdown.
    shutdownHook = ShutdownHookManager.addShutdownHook { () =>
      // It's possible that we arrive here before calling `fetchAndRunExecutor`, then `state` will
      // be `ExecutorState.RUNNING`. In this case, we should set `state` to `FAILED`.
      if (state == ExecutorState.RUNNING) {
        state = ExecutorState.FAILED
      }
      killProcess(Some("Container shutting down"))
    }
  }

  /**
    * Kill executor process, wait for exit and notify worker to update resource status.
    *
    * @param message the exception message which caused the executor's death
    */
  def killProcess(message: Option[String]) {
    var exitCode: Option[Int] = None
    if (process != null) {
      logInfo("Killing process!")
      if (stdoutAppender != null) {
        stdoutAppender.stop()
      }
      if (stderrAppender != null) {
        stderrAppender.stop()
      }
      exitCode = Utils.terminateProcess(process, EXECUTOR_TERMINATE_TIMEOUT_MS)
      if (exitCode.isEmpty) {
        logWarning("Failed to terminate process: " + process +
          ". This process will likely be orphaned.")
      }
      logInfo(s"killProcess method,process: $process")
    }
    try {
      master.send(ExecutorRemove(name, state, message, exitCode, _command.msg, master))
    } catch {
      case ee: IOException => logWarning("Catch Exception while send ExecutorRemove,maybe cause of LeaderElection,so just warn it", ee)
      case e: Exception => logWarning(e.getMessage, e)
    }
  }

  //手动 kill
  /** Stop this executor runner, including killing the process it launched */
  private[orco] def kill() {
    try {
      ShutdownHookManager.removeShutdownHook(shutdownHook)
    } catch {
      case e: IllegalStateException => logError("method kill called,but filed to remove ShutdownHook", e)
    }
    if (process != null) {
      logInfo("Kill process!")
      if (stdoutAppender != null) {
        stdoutAppender.stop()
      }
      if (stderrAppender != null) {
        stderrAppender.stop()
      }
      val exitCode = Utils.terminateProcess(process, EXECUTOR_TERMINATE_TIMEOUT_MS)
      if (exitCode.isEmpty) {
        logWarning("Failed to terminate process: " + process +
          ". This process will likely be orphaned.")
      }
      logInfo(s"kill method,process: $process")
    }

    if (workerThread != null) {
      // the workerThread will kill the child process when interrupted
      workerThread.interrupt()
      logInfo("workerThread.interrupt")
      workerThread = null
    }
    state = ExecutorState.KILLED
  }

  def close(): Unit = {
    try {
      ShutdownHookManager.removeShutdownHook(shutdownHook)
      if (workerThread != null) {
        // the workerThread will kill the child process when interrupted
        workerThread.interrupt()
        workerThread = null
      }
      if (stdoutAppender != null) {
        stdoutAppender.stop()
      }
      if (stderrAppender != null) {
        stderrAppender.stop()
      }
      if (process != null) {
        val exitCode = Utils.terminateProcess(process, EXECUTOR_TERMINATE_TIMEOUT_MS)
        if (exitCode.isEmpty) {
          logWarning("Failed to terminate process: " + process +
            ". This process will likely be orphaned.")
        }
      }
    } catch {
      case _: Exception => None
    }
  }

  /** Replace variables such as {{EXECUTOR_ID}} and {{CORES}} in a command argument passed to us */
  private[worker] def substituteVariables(argument: String): String = argument match {
    //    case "{{WORKER_URL}}" => workerUrl
    //    case "{{HOSTNAME}}" => host
    //    case "{{CORES}}" => cores.toString
    //    case "{{APP_ID}}" => appId
    case other => other
  }

  /**
    * Download and run the executor described in our ApplicationDescription
    */
  private def fetchAndRunExecutor() {
    try {
      // Launch the process
      val builder = CommandUtils.buildProcessBuilder(_command, null, memory, sparkHome.getAbsolutePath, substituteVariables)
      val command = builder.command()
      val formattedCommand = command.asScala.mkString("\"", "\" \"", "\"")
      logInfo(s"Launch command: $formattedCommand")

      if (!executorDir.exists()) {
        executorDir.mkdir()
      }
      builder.directory(executorDir)
      //      builder.environment.put("SPARK_EXECUTOR_DIRS", appLocalDirs.mkString(File.pathSeparator))
      // In case we are running this from within the holder Shell, avoid creating a "scala"
      // parent process for the executor command
      //      builder.environment.put("SPARK_LAUNCH_WITH_SCALA", "0")

      // Add webUI log urls
      //      val baseUrl =
      //        if (conf.getBoolean("holder.ui.reverseProxy", false)) {
      //          s"/proxy/$workerId/logPage/?appId=$appId&executorId=$execId&logType="
      //        } else {
      //          s"http://$publicAddress:$webUiPort/logPage/?appId=$appId&executorId=$execId&logType="
      //        }
      //      builder.environment.put("SPARK_LOG_URL_STDERR", s"${baseUrl}stderr")
      //      builder.environment.put("SPARK_LOG_URL_STDOUT", s"${baseUrl}stdout")

      process = builder.start()
      val header = "holder Executor Command: %s\n%s\n\n".format(formattedCommand, "=" * 40)

      // Redirect its stdout and stderr to files
      val stdout = new File(executorDir, "stdout")
      stdoutAppender = FileAppender(process.getInputStream, stdout, conf)

      val stderr = new File(executorDir, "stderr")
      Files.write(header, stderr, StandardCharsets.UTF_8)
      stderrAppender = FileAppender(process.getErrorStream, stderr, conf)

      // Wait for it to exit; executor may exit with code 0 (when driver instructs it to shutdown)
      // or with nonzero exit code
      val exitCode = process.waitFor()
      Thread.sleep(1000)
      if (state != ExecutorState.KILLED) {
        state = ExecutorState.EXITED
        val message = "Command exited with code " + exitCode
        if (state != ExecutorState.KILLED) {
          try {
            master.send(ExecutorRemove(name, state, Some(message), Some(exitCode), _command.msg, master))
          } catch {
            case ee: IOException => logWarning("Catch Exception while send ExecutorRemove,maybe cause of LeaderElection,so just warn it", ee)
            case e: Exception => logWarning(e.getMessage, e)
          }
        }
      }
    } catch {
      case _: InterruptedException =>
        logInfo("Runner thread for executor " + name + " interrupted")
        state = ExecutorState.KILLED
        killProcess(Some("kill method called"))
      case e: Exception =>
        logError("Error running executor", e)
        state = ExecutorState.FAILED
        killProcess(Some(e.toString))
    }
  }
}

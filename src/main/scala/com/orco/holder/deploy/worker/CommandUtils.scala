package com.orco.holder.deploy.worker

import java.io.{File, FileOutputStream, IOException, InputStream}

import com.orco.holder.deploy.Command
import com.orco.holder.internal.Logging
import com.orco.holder.launcher.WorkerCommandBuilder
import com.orco.holder.util.Utils

import scala.collection.Map
import scala.collection.JavaConverters._

/**
  * Utilities for running commands with the holder classpath.
  */
private[deploy]
object CommandUtils extends Logging {

  /**
    * Build a ProcessBuilder based on the given parameters.
    * The `env` argument is exposed for testing.
    */
  def buildProcessBuilder(command: Command,
                          securityMgr: SecurityManager,
                          memory: Int,
                          sparkHome: String,
                          substituteArguments: String => String,
                          classPaths: Seq[String] = Seq.empty,
                          env: Map[String, String] = sys.env): ProcessBuilder = {
    val localCommand = buildLocalCommand(command, securityMgr, substituteArguments, classPaths, env)
    val commandSeq = buildCommandSeq(localCommand, memory, sparkHome)
    val builder = new ProcessBuilder(commandSeq: _*)
    val environment = builder.environment()
    for ((key, value) <- localCommand.environment) {
      environment.put(key, value)
    }
    builder
  }

  private def buildCommandSeq(command: Command, memory: Int, sparkHome: String): Seq[String] = {
    // holder-698: do not call the run.cmd script, as process.destroy()
    // fails to kill a process tree on Windows
    val cmd = new WorkerCommandBuilder(sparkHome, memory, command).buildCommand()
    cmd.asScala ++ Seq(command.mainClass) ++ command.arguments
  }

  /**
    * Build a command based on the given one, taking into account the local environment
    * of where this command is expected to run, substitute any placeholders, and append
    * any extra class paths.
    */
  private def buildLocalCommand(
                                 command: Command,
                                 securityMgr: SecurityManager,
                                 substituteArguments: String => String,
                                 classPath: Seq[String] = Seq.empty,
                                 env: Map[String, String]): Command = {
    val libraryPathName = Utils.libraryPathEnvName
    val libraryPathEntries = command.libraryPathEntries
    val cmdLibraryPath = command.environment.get(libraryPathName)

    val newEnvironment = if (libraryPathEntries.nonEmpty && libraryPathName.nonEmpty) {
      val libraryPaths = libraryPathEntries ++ cmdLibraryPath ++ env.get(libraryPathName)
      command.environment + ((libraryPathName, libraryPaths.mkString(File.pathSeparator)))
    } else {
      command.environment
    }

    // set auth secret to env variable if needed
    // TODO: 注掉了
    //    if (securityMgr.isAuthenticationEnabled) {
    //      newEnvironment += (SecurityManager.ENV_AUTH_SECRET -> securityMgr.getSecretKey)
    //    }

    Command(
      command.mainClass,
      command.arguments.map(substituteArguments),
      newEnvironment,
      command.classPathEntries ++ classPath,
      Seq.empty, // library path already captured in environment variable
      // filter out auth secret from java options
      // TODO: 注掉了
      command.javaOpts)
    //      command.javaOpts.filterNot(_.startsWith("-D" + SecurityManager.SPARK_AUTH_SECRET_CONF)))
  }

  /** Spawn a thread that will redirect a given stream to a file */
  def redirectStream(in: InputStream, file: File) {
    val out = new FileOutputStream(file, true)
    // TODO: It would be nice to add a shutdown hook here that explains why the output is
    //       terminating. Otherwise if the worker dies the executor logs will silently stop.
    new Thread("redirect output to " + file) {
      override def run() {
        try {
          Utils.copyStream(in, out, true)
        } catch {
          case e: IOException =>
            logInfo("Redirection to " + file + " closed: " + e.getMessage)
        }
      }
    }.start()
  }
}

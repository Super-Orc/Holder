package com.orco.holder.launcher

import java.io.File
import java.util.{HashMap => JHashMap, List => JList, Map => JMap}

import com.orco.holder.deploy.Command
import scala.collection.JavaConverters._

/**
  * This class is used by CommandUtils. It uses some package-private APIs in SparkLauncher, and since
  * Java doesn't have a feature similar to `private[holder]`, and we don't want that class to be
  * public, needs to live in the same package as the rest of the library.
  */
private[holder] class WorkerCommandBuilder(sparkHome: String, memoryMb: Int, command: Command)
  extends AbstractCommandBuilder {

  childEnv.putAll(command.environment.asJava)
  childEnv.put(CommandBuilderUtils.ENV_SPARK_HOME, sparkHome)

  override def buildCommand(env: JMap[String, String]): JList[String] = {
    val cmd = buildJavaCommand(command.classPathEntries.mkString(File.pathSeparator))
    cmd.add(s"-Xmx${memoryMb}M")
    command.javaOpts.foreach(cmd.add)
    cmd
  }

  def buildCommand(): JList[String] = buildCommand(new JHashMap[String, String]())

}
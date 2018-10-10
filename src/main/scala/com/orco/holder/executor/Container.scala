package com.orco.holder.executor

import java.net.InetAddress

import com.loktar.loader.DynamicClassLoader
import com.orco.holder.HolderConf
import com.orco.holder.deploy.HolderCuratorUtil
import com.orco.holder.deploy.master.MasterMessages.{BoundPortsRequest, BoundPortsResponse}
import com.orco.holder.internal.Logging
import com.orco.holder.rpc._
import com.orco.holder.scheduler.cluster.CoarseGrainedClusterMessages.{RegisterContainer, RegisterExecutorFailed, RegisteredExecutor}
import com.orco.holder.util.{HolderUncaughtExceptionHandler, SUtil, ThreadUtils, Utils}

import scala.util.{Failure, Success}

class Container(val rpcEnv: RpcEnv,
                rpcAddress: RpcAddress,
                host: String, //master
                port: Int,
                val conf: HolderConf,
                arg: String,
                date: String) extends ThreadSafeRpcEndpoint with Logging {

  @volatile var master: Option[RpcEndpointRef] = None
  var webUiUrl: String = ""

  override def onStart() {
    logInfo(s"Connecting to master or standby:Master-$host:$port")
    webUiUrl = s"http://$host:$port"
    //    try {
    val address = RpcEndpointAddress(RpcAddress(host, port), s"Master-$host:$port").toString
    rpcEnv.asyncSetupEndpointRefByURI(address).flatMap { ref =>
      master = Some(ref)
      ref.ask[Boolean](RegisterContainer(self, arg, date))
    }(ThreadUtils.sameThread).onComplete {
      // This is a very fast action so we can use "ThreadUtils.sameThread"
      case Success(msg) =>
        logInfo(s"success register with master: $address,msg:$msg")
      // Always receive `true`. Just ignore it
      case Failure(e) =>
        logError(s"Cannot register with master: $address", e)
        //        exitExecutor(1, s"Cannot register with driver: $driverUrl", e, notifyDriver = false)
        sys.exit(1)
    }(ThreadUtils.sameThread)
    //    } catch {
    //      case _: RpcEndpointNotFoundException =>
    //        try {
    //          logError(s"Can not find Master-$host:$port,try to find StandBy-$host:$port.")
    //          val add = RpcEndpointAddress(RpcAddress(host, port), s"StandBy-$host:$port").toString
    //          connect(add)
    //        } catch {
    //          case e: Exception =>
    //            logError(s"Can not find StandBy-$host:$port,System exit.", e)
    //        }
    //      case i: Exception =>
    //        logError("Container onStart error,System exit.", i)
    //        sys.exit(2)
    //    }
  }

  override def receive: PartialFunction[Any, Unit] = {
    case RegisteredExecutor =>
      try {
        run(arg)
      } catch {
        case e: Exception =>
          logError("Job run error,exit", e)
          sys.exit(3)
      }
    case RegisterExecutorFailed =>
      logError("Container get response : RegisterExecutorFailed,Container is gonna be killed")
      sys.exit(4)

  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {

    case BoundPortsRequest =>
      logInfo("BoundPortsRequest->BoundPortsResponse")
      context.reply(BoundPortsResponse(rpcAddress.port, 8080, null))
  }

  def run(msg: String): Unit = {
    val message = SUtil.beObject(msg)
    val classloader = new DynamicClassLoader(Thread.currentThread().getContextClassLoader)
    val clazz = classloader.loadClass(message.getString("classPackage"))
    //    OrcoClassLoader.loadJar(jar.getJarPath, jar.getJarName)
    //    val clazz = OrcoClassLoader.loadClass(jar.getJarName, jar.getClassPackage)
    val method = clazz.getDeclaredMethod(message.getString("method"))
    method.setAccessible(true)
    method.invoke(clazz.newInstance())
  }
}

object Container extends Logging {

  var SYSTEM_NAME = "lok-tar"
  val ENDPOINT_NAME_CONTAINER = "Container"
  var arg: String = _
  var date: String = _

  def main(args: Array[String]) {
    Thread.setDefaultUncaughtExceptionHandler(new HolderUncaughtExceptionHandler(
      exitOnUncaughtException = false))
    Utils.initDaemon(log)

    var argv = args.toList
    while (argv.nonEmpty) {
      argv match {
        case ("--message") :: value :: tail =>
          arg = value
          argv = tail
        case ("--date") :: value :: tail =>
          date = value
          argv = tail
        case Nil =>
        case tail =>
          System.err.println(s"Unrecognized options: ${tail.mkString(" ")}")
          printUsageAndExit()
      }
    }
    val conf = new HolderConf
    val host = InetAddress.getLocalHost.getHostAddress
    val port = HolderCuratorUtil.getNextPort

    val (masterHost, masterPort) = HolderCuratorUtil.checkExists("Master")
    val (rpcEnv, _, _) =
      if (masterHost != null) {
        //masterSb
        SYSTEM_NAME = SYSTEM_NAME + s"-$host:$port"
        startRpcEnvAndEndpoint(host, port, masterHost, masterPort, conf, arg, date)
      } else {
        //        //master
        //        SYSTEM_NAME = SYSTEM_NAME + s"-$host:$port"
        //        startRpcEnvAndEndpoint(host, port, null, 0, conf)
        logError("init container failed because of master not found")
        sys.exit(5)
      }
    rpcEnv.awaitTermination()
    sys.exit(6)
  }

  def startRpcEnvAndEndpoint(
                              host: String,
                              port: Int,
                              masterHostOrNull: String,
                              masterPortOrZero: Int,
                              conf: HolderConf,
                              args: String,
                              date: String): (RpcEnv, Int, Option[Int]) = {
    val rpcEnv = RpcEnv.create(SYSTEM_NAME, host, port, conf)
    val endpoint_name = ENDPOINT_NAME_CONTAINER + s"-$host:$port"
    val masterEndpoint = rpcEnv.setupEndpoint(endpoint_name, new Container(rpcEnv, rpcEnv.address, masterHostOrNull, masterPortOrZero, conf, args, date))
    val portsResponse = masterEndpoint.askSync[BoundPortsResponse](BoundPortsRequest)
    (rpcEnv, portsResponse.webUIPort, portsResponse.restPort)
  }

  private def printUsageAndExit(): Unit = {
    System.err.println(
      """
        |Usage: Container [options]
        |
        | Options are:
        |   --message <ReMessage>
        |""".stripMargin)
    System.exit(7)
  }
}
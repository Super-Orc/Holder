package com.orco.holder.deploy.master

import java.io.File
import java.net.InetAddress
import java.text.SimpleDateFormat
import java.util.concurrent.{ArrayBlockingQueue, ConcurrentHashMap, TimeUnit, Future => JFuture, ScheduledFuture => JScheduledFuture}
import java.util.{Date, Locale}

import com.orco.cache.RedisUtil
import com.orco.holder.{HolderConf, HolderException}
import com.orco.holder.deploy.DeployMessages._
import com.orco.holder.deploy.{Command, ExecutorState, HolderCuratorUtil, WatchLevel}
import com.orco.holder.deploy.master.MasterMessages._
import com.orco.holder.deploy.master.ui.MasterWebUI
import com.orco.holder.deploy.worker.ExecutorRunner
import com.orco.holder.executor.ContainerInfo
import com.orco.holder.internal.Logging
import com.orco.holder.rpc._
import com.orco.holder.scheduler.cluster.CoarseGrainedClusterMessages._
import com.orco.holder.serializer.JavaSerializer
import com.orco.holder.util.{HolderUncaughtExceptionHandler, ThreadUtils, Utils}
import redis.clients.jedis.{Jedis, JedisPubSub}
import com.orco.holder.util.SUtil._

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

/**
  * 1、修改 redis ip 和 port（非必须）
  * 2、修改 HolderConf 中 zk 地址
  * 3、修改 sparkHome （非必须）
  * 4、修改 zk 持久化的路径
  * 5、修改 OrcoClassLoader 中的 DEFAULT_LOCAL_DIR
  * 6、修改 DynamicClassLoader 中的 DYNAMIC_JARS_DIR_KEY
  * NOTE: 一个 jar 包重复运行多次怎么办
  */
private[deploy] class Master(val rpcEnv: RpcEnv,
                             address: RpcAddress,
                             //                             host: String,
                             //                             port: Int,
                             val conf: HolderConf,
                             val webUiPort: Int = 12000)
  extends ThreadSafeRpcEndpoint with Logging with LeaderElectable {

  private def createDateFormat = new SimpleDateFormat("yyyyMMddHHmmss", Locale.US)

  /**
    * some param must be done when master changed
    */
  private var thisId: String = _
  private var masterHost: String = _
  private var masterPort: Int = _
  private var masterName: String = _
  private var registeredToMaster = false
  private var master: Option[RpcEndpointRef] = None
  private val masterSbSet = new mutable.HashSet[MasterStandByInfo]
  private val idToMasterSb = new mutable.HashMap[String, MasterStandByInfo]
  private var connected = false
  private var containerMap = new ConcurrentHashMap[String, ContainerInfo]
  private var containerRestartCount = new ConcurrentHashMap[String, Integer]
  val finishedContainer = new ArrayBlockingQueue[ContainerInfo](3)
  /**
    * 静态不变的
    */
  private val masterPrefix = Master.ENDPOINT_NAME_MASTER
  private val masterStandByPrefix = Master.ENDPOINT_NAME_STANDBY
  private val containerPrefix = "Container_"
  private val sparkHome = "/Users/orco/test"
  //  private val sparkHome = "/opt/zf/holder"
  var workDir: File = _


  //  implicit val executorContext: ExecutionContextExecutorService = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(5))

  // After onStart, webUi will be set
  private var webUi: MasterWebUI = _
  private var webUiUrl: String = ""

  private val HEARTBEAT_MILLIS = conf.getLong("holder.worker.timeout", 60) * 1000 / 4
  private val redisScheduler = ThreadUtils.newDaemonSingleThreadExecutor("redis-subscribe-thread")
  private val topics = "orco"

  private var persistenceEngine: PersistenceEngine = _
  private var leaderElectionAgent: LeaderElectionAgent = _
  private val INITIAL_REGISTRATION_RETRY_INTERVAL_SECONDS = 30L
  private val PROLONGED_REGISTRATION_RETRY_INTERVAL_SECONDS = 60L
  private var connectionAttemptCount = 0
  private val INITIAL_REGISTRATION_RETRIES = 1
  private val TOTAL_REGISTRATION_RETRIES = INITIAL_REGISTRATION_RETRIES + 3

  private var forwardMessageScheduler = ThreadUtils.newDaemonSingleThreadScheduledExecutor("master-masterStandBy")
  private var heartBeatScheduler = ThreadUtils.newDaemonSingleThreadScheduledExecutor("heartBeat-thread-pool")
  private var registerMasterThreadPool = ThreadUtils.newDaemonCachedThreadPool("masterStandBy-register-to-master-thread-pool", 5)

  private var registerMasterFutures: JFuture[_] = _
  private var registrationRetryTimer: Option[JScheduledFuture[_]] = None


  private def createWorkDir() {
    workDir = new File(sparkHome, "work")
    try {
      // This sporadically fails - not sure why ... !workDir.exists() && !workDir.mkdirs()
      // So attempting to create and then check if directory was created or not.
      workDir.mkdirs()
      if (!workDir.exists() || !workDir.isDirectory) {
        logError("Failed to create logs directory " + workDir)
        System.exit(1)
      }
      assert(workDir.isDirectory)
    } catch {
      case e: Exception =>
        logError("Failed to create logs directory " + workDir, e)
        System.exit(1)
    }
  }

  def init(masterRef: Option[RpcEndpointRef]): Unit = {
    try {
      masterRef match {
        case Some(master_) =>
          //master change ,refresh state
          refreshMasterState(master_)
        case None =>
          //first time init
          val masterRef = persistenceEngine.read[MasterInfo](masterPrefix).head.masterRef
          refreshMasterState(masterRef)
      }
      runUI()
      registerToMaster()
    } catch {
      case e: Exception => logError(s"masterStandBy init error", e)
        sys.exit(1)
    }
  }

  override def onStart(): Unit = {
    createWorkDir()
    val serializer: JavaSerializer = new JavaSerializer(conf)
    val zkFactory = new ZooKeeperRecoveryModeFactory(conf, serializer)
    //todo 这里感觉有点重复，object master 那里已经获得了 master 的 host 了
    persistenceEngine = zkFactory.createPersistenceEngine()
    leaderElectionAgent = zkFactory.createLeaderElectionAgent(this)

    if (persistenceEngine.checkExists(masterPrefix)) {
      //masterStandBy
      thisId = generateMasterId(masterStandByPrefix, address.host, address.port)
      init(None)
    } else {
      // master
      persistenceEngine.cleanZK()
      master = Some(self)
      masterHost = address.host
      masterPort = address.port
      masterName = Master.ENDPOINT_NAME_MASTER + s"-$masterHost:$masterPort"
      thisId = generateMasterId(masterPrefix, masterHost, masterPort)
      persistenceEngine.addMaster(new MasterInfo(thisId, masterHost, masterPort, masterName, self))
      runUI()
    }

    heartBeatScheduler.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = Utils.tryLogNonFatalError {
        //                println(createDateFormat.format(new Date) + "->" + containerMap)
        //        println(containerMap)
        //        println(masterHost + ":" + masterPort)
        //        containerMap.values().map(a => (a.name, a.executor.workerThread)).foreach(println(_))
      }
    }, 0, 3, TimeUnit.SECONDS)

    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit = {
        if (isMaster) {
          leaderElectionAgent.stop()
          //          persistenceEngine.removeMaster(thisId)
          logInfo("master killing... zookeeper persistence won't clean soon...")
        } else {
          leaderElectionAgent.stop()
          //              persistenceEngine.removeMasterStandBy(thisId)
          logInfo("masterSb killing... zookeeper persistence clean soon...")
        }
      }
    })
  }

  override def onStop() {
    webUi.stop()
    persistenceEngine.close()
    leaderElectionAgent.stop()
  }

  override def electedLeader() {
    self.send(ElectedLeader)
  }

  override def revokedLeadership() {
    self.send(RevokedLeadership)
  }

  // TODO: 周四上午，这里可以发现 master 挂掉与否,或者是某个机器挂掉与否，或者是 container 挂掉与否，否可以尝试在这里重启？
  //这里还要考虑，如果有多个 standby 是否需要过滤去重之类的
  /**
    * 我要考虑的是，master 挂掉后，在新的 master 上要把老 master 的 container 启动起来
    * 方法一、
    */
  override def onDisconnected(remoteAddress: RpcAddress): Unit = {

    if (masterHost == remoteAddress.host && masterPort == remoteAddress.port) {
      logInfo(s"Master endpoint $remoteAddress Disconnected!")
      registeredToMaster = false
      connected = false
      //      standBylostMaster()
      //todo 这里要逐渐废弃，如果 standby 离线，通过钩子去发一个离线信息
    } else if (masterSbSet.map(m => new RpcAddress(m.host, m.port)).contains(remoteAddress)) {
      logInfo(s"masterStandBy endpoint $remoteAddress Disconnected!")
      for (m <- masterSbSet if m.host == remoteAddress.host && m.port == remoteAddress.port) {
        masterSbSet -= m
        idToMasterSb -= m.id
        //todo 这里要重启 masterStandBy
      }
    }
    else {
      logInfo(s"onDisconnected ignored,$remoteAddress")
    }
  }

  //todo master 挂掉，就在这里重启，standby 挂掉，在 onDisconnected 重启
  override def receive: PartialFunction[Any, Unit] = {
    case ElectedLeader =>
      if (isMaster) {
        logInfo(s"i am master,receive.ElectedLeader will do nothing,$address")
        redisSub()
        //        runUI()
      } else {
        val oldMasterHost = masterHost
        val oldMasterPort = masterPort
        logInfo(s"i am becoming master,receive.ElectedLeader will do something,$address,old Master is : $masterHost:$masterPort")
        //        runUI()

        /**
          * 第一次 master 会进来
          * masterStandBy 会进来，并在这里成为新的 master
          * 我先把老的 master 路径移除了，在添加新的 master 路径，同时移除 this masterStandBy 路径
          * 然后在这里重启挂掉的节点，启动一个定时器，每隔3s 去 restart 挂掉的节点，如果节点上线，向 master 注册，则 shutdown 该定时器
          * 一会考虑下，多个 standBy，这里如何处理,多个的话，这里只会有一个升级为 master 的 standby 进来
          */
        masterHost = address.host
        masterPort = address.port

        // TODO: 待确认，从挂了，会发生选举吗
        // TODO: 主挂了，在这里把主的 container 启动起来
        // TODO: 从挂了，从上的 container 会自动启动起来吗

        val masterInfo: Seq[MasterInfo] = persistenceEngine.read[MasterInfo](masterPrefix)
        if (masterInfo.nonEmpty) persistenceEngine.removeMaster(masterInfo.head.id)

        persistenceEngine.removeMasterStandBy(thisId)

        thisId = generateMasterId(masterPrefix, masterHost, masterPort)

        masterName = s"${Master.ENDPOINT_NAME_MASTER}-$masterHost:$masterPort"
        persistenceEngine.addMaster(new MasterInfo(thisId, masterHost, masterPort, masterName, self))

        master = Some(self)

        cancelLastRegistrationRetry()
        heartBeatScheduler.shutdown()


        //获取所有 masterStandBy ,并通知他们，产生了新的 master
        //最好是在 onDisconnected 里面通知
        //还是主动去通知他们吧，因为 onDisconnected 比 ElectedLeader 快
        //最后发现还是 onDisconnected 慢
        val masterStandByInfo: Seq[MasterStandByInfo] = persistenceEngine.read[MasterStandByInfo](masterStandByPrefix)
        for (m <- masterStandByInfo) {
          val masterStandByEndpoint = rpcEnv.setupEndpointRef(RpcAddress(m.host, m.port), m.name)
          masterStandByEndpoint.send(MasterChanged(self))
        }
        redisSub()

        // TODO: 在这里启动挂掉机器的 container
        logInfo(s"Before ElectedLeader,containerMap:$containerMap")
        //        persistenceEngine.read[ContainerInfo](containerPrefix).foreach(bean => containerMap.put(bean.executor.name, bean))
        containerMap.values().filter(_.nodeId.contains(s"$oldMasterHost-$oldMasterPort"))
          .foreach(c => runContainer(c.message))

        // TODO: 周六来了后，注意这里 master 状态的改变，因为主节点挂掉后，kill app 会报错，executorrunner 那里，executorremove 会找不到15000
        //最新，重新选举后，master 的已经可以了，但是另一个 standby 的 kill 报错
        containerMap.foreach(_._2.executor.master = master.get)
        //        containerMap.foreach(container => {
        //          container._2.nodeId = null
        //          runContainer(container._2.message)
        //        })
        logInfo(s"After ElectedLeader,containerMap:$containerMap")
      }

    //masterSb
    case MasterChanged(masterRef) =>
      cancelLastRegistrationRetry()
      init(Some(masterRef))

    //masterSb
    case msg: RegisterMasterSbResponse =>
      handleRegisterResponse(msg)

    //master
    case Heartbeat(masterSbId, masterSb) =>
      idToMasterSb.get(masterSbId) match {
        case Some(masterStandByInfo) =>
          logInfo(s"master get a heartbeat signal $masterStandByInfo")
        case None =>
          logError(s"Got heartbeat from unregistered masterSb $masterSb." +
            s" This masterSb was never registered, so i don't know how to handle this heartbeat. -->${WatchLevel.LEVEL_HIGH}<--")
      }
    //masterSb
    case SendHeartbeat =>
      if (connected) {
        sendToMaster(Heartbeat(thisId, self))
      }

    //master
    case RegisterMasterSb(id, masterSbHost, masterSbPort, masterSbRef, standByWebUiUrl, masterAddress) =>
      logInfo("Registering by MasterSb %s:%d ".format(masterSbHost, masterSbPort))
      if (idToMasterSb.contains(id)) {
        masterSbRef.send(RegisterMasterSbFailed(s"Duplicate masterSb ID $id"))
      } else {
        val masterSb = new MasterStandByInfo(id, masterSbHost, masterSbPort, masterSbRef.name, masterSbRef, standByWebUiUrl)
        masterSbRef.send(RegisteredMasterSb(masterSb, masterAddress))
        idToMasterSb(id) = masterSb
        masterSbSet += masterSb
        containerMap.foreach(c => broadcastContainer(c._1, c._2, "ONLINE"))
      }

    //masterSb
    case ReregisterToMaster =>
      reRegisterToMaster()

    //master
    // TODO: 选举后，如果老 master 来了，则忽视？
    case ExecutorRemove(name, state, errMsg, exitStatus, message, _master) =>
      _master.ask[Boolean](MasterExistRequest).onComplete {
        case Success(_) =>
          logInfo(s"ExecutorRemove,success find with master: ${_master.address}")
          logInfo(s"Container state changed:${ExecutorRemove(name, state, errMsg, exitStatus, message, _master)}") //这样能打印吗
          logInfo(s"ExecutorRemove->$containerMap")

          val info: ContainerInfo = containerMap.get(name)
          if (containerRestartCount.get(name) == null) {
            containerRestartCount.put(name, 0)
          }
          if (info != null) {
            containerRestartCount.put(name, containerRestartCount.get(name) + 1)
            persistenceEngine.removeContainer(containerMap.get(name).id)
            containerRemove(name, info)
            broadcastContainer(name, null, "OFFLINE")
          }

          if (ExecutorState.isFinished(state)) {

            if (containerRestartCount.get(name) <= 3) {
              logInfo(s"Restart Container ${containerRestartCount.get(name)} times,Container: $errMsg")
              runContainer(message)
            } else {
              logInfo(s"Restart Container times:${containerRestartCount.get(name)},more than three times,cancel restart")
              // TODO: 这里的变量应该有个存活时长
              containerRestartCount.remove(name)
            }
          } else {
            containerRestartCount.remove(name)
          }
        case Failure(e) =>
          logError(s"ExecutorRemove,Cannot find with master: ${_master.address}", e)
      }(ThreadUtils.sameThread)


    case ContainerOnLine(name, containerInfo) =>
      logInfo(s"ContainerOnLine->containerMap:$containerMap")
      //      if(isMaster){
      //        containerInfo.webUiUrl =
      //      }
      val info = containerMap.get(name)
      if (info != null && info.executor.valid != null) {
        containerInfo.executor = info.executor
        containerInfo.executor.master = master.get
        containerInfo.webUiUrl = webUiUrl
        containerMap.put(name, containerInfo)
      } else {
        containerMap.put(name, containerInfo)
      }
      //      if (containerInfo.isLocal && containerMap.get(name) != null) {
      //      } else {
      //      }
      logInfo(s"ContainerOnLine->containerMap:$containerMap")

    //standby
    case ContainerOffLine(name) =>
      logInfo(s"ContainerOffLine->containerMap:$containerMap")
      if (containerMap.containsKey(name)) {
        val info = containerMap.get(name)
        if (info != null) {
          containerRemove(name, info)
        }
      }
      logInfo(s"ContainerOffLine->containerMap:$containerMap")

  }

  //master or standby
  private def containerRemove(name: String, info: ContainerInfo): Unit = {
    if (info.isLocal) {
      while (finishedContainer.size >= 3) {
        finishedContainer.poll()
      }
      finishedContainer.offer(info)
    }
    containerMap.remove(name)
    info.markFinished()
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {

    case BoundPortsRequest =>
      logInfo("BoundPortsRequest->BoundPortsResponse")
      context.reply(BoundPortsResponse(address.port, 8080, null))

    //master
    case RegisterContainer(containerRef, message, _date) =>
      val msg = beObject(message)
      val name = msg.getString("name")
      try {
        logInfo(s"RegisterContainer->$containerMap")
        val containerInfo = containerMap.get(name)
        containerInfo.id = generateMasterId(s"$containerPrefix$name", containerRef.address.host, containerRef.address.port, _date)
        containerInfo.webUiUrl = webUiUrl
        containerInfo.masterWebUrl = webUiUrl
        persistenceEngine.addContainer(containerInfo)
        containerRef.send(RegisteredExecutor)
        broadcastContainer(name, containerInfo, "ONLINE")
        logInfo(s"Receive RegisterContainer->$containerMap")
        context.reply(true)
      } catch {
        case e: Exception =>
          containerRef.send(RegisterExecutorFailed)
          context.reply(false)
          logError(s"RegisterContainer failed,try to restart it,$name", e)
          runContainer(message)
          //          val msg = beObject(message)
          //          val name = msg.getString("name")
          self.send(ExecutorRemove(name, ExecutorState.FAILED, Some("RegisterContainer catch exception"), Some(55), message, self))
      }

    case MasterWebUIRequest =>
      context.reply(MasterWebUIResponse(webUiUrl))

    //master or standby
    case RequestMasterState =>
      context.reply(MasterStateResponse(
        address.host, address.port, isMaster, if (isMaster) webUiUrl else master.get.askSync[MasterWebUIResponse](MasterWebUIRequest).masterWebUI,
        masterSbSet.toArray, containerMap.values.toList, finishedContainer.toList))

    case MasterExistRequest =>
      context.reply(true)

    //standby
    case ContainerRunningRequest(message) =>
      try {
        runningContainer(message)
        context.reply(true)
      } catch {
        case e: Exception =>
          logError(s"Receive ContainerRunningRequest:$message,RunningContainer false", e)
          context.reply(false)
      }

    //master
    case NewContainerRequest(info) =>
      try {
        containerMap.put(info.name, info)
        context.reply(true)
      } catch {
        case e: Exception => context.reply(false)
      }
  }

  private def broadcastContainer(name: String, containerInfo: ContainerInfo, flag: String): Unit = {

    logInfo(s"Method broadcastContainer,$flag,$containerInfo")
    flag match {
      case "ONLINE" => idToMasterSb.foreach(_._2.standByRef.send(ContainerOnLine(name, containerInfo)))
      case "OFFLINE" => idToMasterSb.foreach(_._2.standByRef.send(ContainerOffLine(name)))
    }
  }


  def runContainer(message: String): Unit = {
    assert(isMaster, "Method runContainer only master can in")
    val _id: String = {
      val nodeIds = containerMap.values().map(_.nodeId)
      var id: String = null
      var boo = true
      if (!nodeIds.contains(thisId)) {
        id = thisId
      } else {
        for (a <- masterSbSet if boo) {
          if (!nodeIds.exists(_.contains(s"${a.host}-${a.port}"))) {
            id = a.id
            boo = false
          }
        }
      }
      id
    }

    if (containerMap.isEmpty) {
      logInfo(s"1Run container in node: ${_id}")
      runningContainer(message)
    }
    else if (_id != null) {
      logInfo(s"2Run container in node: ${_id}")
      if (_id == thisId) runningContainer(message) else askToRun(_id, message)
    }
    else {
      val nodeId: String = containerMap.values().groupBy(_.nodeId).map(c => (c._1, c._2.size)).toList.minBy(_._2)._1
      if (nodeId == thisId) {
        logInfo(s"3Run container in node: ${_id}")
        runningContainer(message)
      } else {
        logInfo(s"4Run container in node: ${_id}")
        askToRun(nodeId, message)
      }
    }
  }

  //master
  def askToRun(nodeId: String, message: String): Unit = {

    idToMasterSb(nodeId).standByRef.ask[Boolean](ContainerRunningRequest(message)).onComplete {
      case Success(_) =>
        logInfo(s"Run Container Success in Node:$nodeId")
      case Failure(_) =>
        // TODO: 这里还没有加重试机制
        logInfo(s"Run Container Failure in Node:$nodeId")
    }(ThreadUtils.sameThread)
  }

  //master or standby
  def runningContainer(message: String): Unit = {
    val msg = beObject(message)
    val name = msg.getString("name")
    logInfo(s"Try to start Container $name")
    val executorEnvs = mutable.HashMap[String, String]()
    val date = createDateFormat.format(new Date)
    val fileName = s"$containerPrefix$name-$date"
    val memory = 512
    val command = Command("com.orco.holder.executor.Container", Seq("--message", message, "--date", date), executorEnvs ++= conf.getExecutorEnv, Seq(), Seq(), Seq())
    val executor = new ExecutorRunner(name, command, memory, if (isMaster) self else master.get, new File(sparkHome), new File(sparkHome + "/work", fileName), conf, ExecutorState.RUNNING)
    logInfo(s"runContainer,containerMap->$containerMap")
    containerMap.put(name, new ContainerInfo(null, thisId, name, fileName, System.currentTimeMillis(), message, executor, null, memory))
    containerRestartCount.put(name, 1)
    if (isMaster) {
      executor.start()
    } else {
      master match {
        case Some(ref) =>
          ref.ask[Boolean](NewContainerRequest(containerMap.get(name))).onComplete {
            // This is a very fast action so we can use "ThreadUtils.sameThread"
            case Success(_) =>
              executor.start()
            case Failure(e) =>
              logError(s"RunningContainer return Failure", e)
          }(ThreadUtils.sameThread)
        case None =>
          logWarning(
            s"Dropping RunningContainer because the connection to master has not yet been established")
      }
    }
  }

  def stopContainer(message: String): Unit = {
    try {
      val msg = beObject(message)
      logInfo(s"Try to stop Container ${msg.getString("name")}")
      containerMap.get(msg.getString("name")).executor.kill()
    } catch {
      case e: Exception => logError(s"Fail to stop Container,$message", e)
    }

  }

  private def redisSub(): Unit = {
    redisScheduler.submit(new Runnable {
      override def run(): Unit = {
        try {
          val jedis: Jedis = RedisUtil.pool.getResource
          val subTopic = jedis.lrange(topics, 0, -1)
          logInfo(s"start Redis Sub...,topic: $subTopic")
          jedis.subscribe(new JedisPubSub {
            override def onMessage(channel: String, message: String): Unit = {
              channel match {
                case "orco:start" => tryCatch(runContainer(message))
                case "orco:stop" => tryCatch(stopContainer(message))
              }
            }
          }, subTopic: _*)
        } catch {
          case e: Exception =>
            logError("startRedisSubscribe error", e)
        }
      }
    })
  }

  private def generateMasterId(name: String,
                               host: String,
                               port: Int,
                               date: String = createDateFormat.format(new Date)): String = {
    "%s-%s-%s-%d".format(name, date, host, port)
  }

  def initPool(): Unit = {
    if (registerMasterThreadPool == null) {
      registerMasterThreadPool = ThreadUtils.newDaemonCachedThreadPool("masterStandBy-register-to-master-thread-pool", 5)
    }
    if (forwardMessageScheduler == null) {
      forwardMessageScheduler = ThreadUtils.newDaemonSingleThreadScheduledExecutor("master-masterStandBy")
    }
    if (heartBeatScheduler == null) {
      heartBeatScheduler = ThreadUtils.newDaemonSingleThreadScheduledExecutor("heartBeat-thread-pool")
    }
  }

  def cancelLastRegistrationRetry(): Unit = {
    if (registerMasterFutures != null) {
      registerMasterFutures.cancel(true)
      registerMasterFutures = null
    }
    if (forwardMessageScheduler != null) {
      forwardMessageScheduler.shutdown()
      forwardMessageScheduler = null
    }
    registrationRetryTimer.foreach(_.cancel(true))
    registrationRetryTimer = None
  }

  /**
    * Send a message to the current master. If we have not yet registered successfully with any
    * master, the message will be dropped.
    */
  private def sendToMaster(message: Any): Unit = {
    master match {
      case Some(masterRef) => masterRef.send(message)
      case None =>
        logWarning(
          s"Dropping $message because the connection to master has not yet been established")
    }
  }


  //masterSb
  private def handleRegisterResponse(msg: RegisterMasterSbResponse): Unit = synchronized {
    msg match {
      case RegisteredMasterSb(masterStandByInfo, masterAddress) =>
        logInfo("Successfully registered to master " + masterAddress.toString)
        registeredToMaster = true
        connected = true
        cancelLastRegistrationRetry()
        //        val masterSb = new MasterStandByInfo(id, masterSbHost, masterSbPort, masterSbRef.name, masterSbRef)
        //before add a node,remove similar node first
        persistenceEngine.addMasterStandBy(masterStandByInfo)

        heartBeatScheduler.scheduleAtFixedRate(new Runnable {
          override def run(): Unit = Utils.tryLogNonFatalError {
            //            self.send(SendHeartbeat)
            //            println(containerMap)
          }
        }, 0, HEARTBEAT_MILLIS, TimeUnit.MILLISECONDS)
      case RegisterMasterSbFailed(message) =>
        logError("Worker registration failed: " + message)
        System.exit(1)
    }
  }


  //masterSb
  private def registerToMaster() {
    initPool()
    // onDisconnected may be triggered multiple times, so don't attempt registration
    // if there are outstanding registration attempts scheduled.
    registrationRetryTimer match {
      case None =>
        registeredToMaster = false
        //        master = Some(masterRef)
        registerMasterFutures = tryRegisterAllMasters()
        connectionAttemptCount = 0
        registrationRetryTimer = Some(forwardMessageScheduler.scheduleAtFixedRate(
          new Runnable {
            override def run(): Unit = Utils.tryLogNonFatalError {
              Option(self).foreach(_.send(ReregisterToMaster))
            }
          },
          INITIAL_REGISTRATION_RETRY_INTERVAL_SECONDS,
          INITIAL_REGISTRATION_RETRY_INTERVAL_SECONDS + 3,
          TimeUnit.SECONDS))
      case Some(_) =>
        logInfo("Not spawning another attempt to register with the master, since there is an" +
          " attempt scheduled already.")
    }
  }

  // masterSb
  private def tryRegisterAllMasters(): JFuture[_] = {
    registerMasterThreadPool.submit(new Runnable {
      override def run(): Unit = {
        try {
          logInfo(s"Connecting to master:$masterName, $masterHost:$masterPort...")
          val masterEndpoint: RpcEndpointRef = rpcEnv.setupEndpointRef(RpcAddress(masterHost, masterPort), masterName)
          logInfo("tryRegisterAllMasters 2")
          refreshMasterState(masterEndpoint)
          sendRegisterMessageToMaster(masterEndpoint)
        } catch {
          case _: InterruptedException => logError("tryRegisterAllMasters interrupted")
          case NonFatal(e) => logWarning(s"Failed to connect to master $masterHost:$masterPort", e)
        }
      }
    })
  }

  def refreshMasterState(masterRef: RpcEndpointRef): Unit = {
    if (masterRef == null) {
      throw new HolderException("The standBy start init,bug master deserialize from zk is null")
    }
    master = Some(masterRef)
    masterHost = master.get.address.host
    masterPort = master.get.address.port
    masterName = master.get.name
  }

  // masterSb
  private def sendRegisterMessageToMaster(masterEndpoint: RpcEndpointRef): Unit = {
    masterEndpoint.send(RegisterMasterSb(
      thisId,
      address.host,
      address.port,
      self,
      webUiUrl,
      masterEndpoint.address))
  }

  private def reRegisterToMaster(): Unit = {
    Utils.tryOrExit {
      connectionAttemptCount += 1
      if (registeredToMaster) {
        cancelLastRegistrationRetry()
      } else if (connectionAttemptCount <= TOTAL_REGISTRATION_RETRIES) {
        master match {
          case Some(masterRef) =>
            // registered == false && master != None means we lost the connection to master, so
            // masterRef cannot be used and we need to recreate it again. Note: we must not set
            // master to None due to the above comments.
            if (registerMasterFutures != null) {
              registerMasterFutures.cancel(true)
            }
            val masterAddress = masterRef.address
            registerMasterFutures = registerMasterThreadPool.submit(new Runnable {
              override def run(): Unit = {
                try {
                  logInfo("Connecting to master " + masterAddress + "...")
                  val masterEndpoint = rpcEnv.setupEndpointRef(masterAddress, masterName)
                  sendRegisterMessageToMaster(masterEndpoint)
                } catch {
                  case _: InterruptedException => // Cancelled
                  case NonFatal(e) => logWarning(s"Failed to connect to master $masterAddress", e)
                }
              }
            })
          case None =>
            if (registerMasterFutures != null) {
              registerMasterFutures.cancel(true)
            }
            // We are retrying the initial registration
            registerMasterFutures = tryRegisterAllMasters()
        }
        // We have exceeded the initial registration retry threshold
        // All retries from now on should use a higher interval
        if (connectionAttemptCount == INITIAL_REGISTRATION_RETRIES) {
          registrationRetryTimer.foreach(_.cancel(true))
          registrationRetryTimer = Some(forwardMessageScheduler.scheduleAtFixedRate(new Runnable {
            override def run(): Unit = Utils.tryLogNonFatalError {
              self.send(ReregisterToMaster)
            }
          }, PROLONGED_REGISTRATION_RETRY_INTERVAL_SECONDS,
            PROLONGED_REGISTRATION_RETRY_INTERVAL_SECONDS,
            TimeUnit.SECONDS))
        }
      } else {
        logError("All masters are unresponsive! Giving up.")
        System.exit(1)
      }
    }
  }

  def isMaster: Boolean = address.host == masterHost && address.port == masterPort

  def restartChild(getPath: String): Unit = {
    //     RmtShellExecutor.getIns.runShell(conf)
  }

  def containers(): ConcurrentHashMap[String, ContainerInfo] = {
    containerMap
  }

  def runUI(): Unit = {
    //    if (isMaster) {
    webUi = new MasterWebUI(this, workDir, webUiPort)
    webUi.bind()
    webUiUrl = s"http://${rpcEnv.address.host}:${webUi.boundPort}"
    logInfo("Master web ui started:" + webUiUrl)
    //    } else {
    //      // TODO: 这里是要用 masterwebui 还是重新弄一个新的页面？
    //      webUi = new WorkerWebUI(this, workDir, webUiPort)
    //      webUi.bind()
    //      webUiUrl = s"http://${rpcEnv.address.host}:${webUi.boundPort}"
    //      logInfo("Standby web ui started:" + webUiUrl)
    //    }
  }


}

private[deploy] object Master extends Logging {
  var SYSTEM_NAME = "lok-tar"
  val ENDPOINT_NAME_MASTER = "Master"
  val ENDPOINT_NAME_STANDBY = "StandBy"

  def main(argStrings: Array[String]) {
    Thread.setDefaultUncaughtExceptionHandler(new HolderUncaughtExceptionHandler(
      exitOnUncaughtException = false))
    Utils.initDaemon(log)
    val conf = new HolderConf
    //    HolderCuratorUtil.newClient(conf)

    val host = InetAddress.getLocalHost.getHostAddress
    val port = HolderCuratorUtil.getNextPort

    val (masterHost, masterPort) = HolderCuratorUtil.checkExists("Master")
    val (rpcEnv, _, _) =
      if (masterHost != null) {
        //masterSb
        SYSTEM_NAME = SYSTEM_NAME + s"-$host:$port"
        startRpcEnvAndEndpoint(host, port, masterHost, masterPort, conf)
      } else {
        //master
        SYSTEM_NAME = SYSTEM_NAME + s"-$host:$port"
        startRpcEnvAndEndpoint(host, port, null, 0, conf)
      }
    rpcEnv.awaitTermination()
  }

  /**
    * Start the Master and return a three tuple of:
    * (1) The Master RpcEnv
    * (2) The web UI bound port
    * (3) The REST server bound port, if any
    */
  def startRpcEnvAndEndpoint(
                              host: String,
                              port: Int,
                              masterHostOrNull: String, //if null,host is master
                              masterPortOrZero: Int,
                              conf: HolderConf): (RpcEnv, Int, Option[Int]) = {
    println(s"$host:$port")
    val rpcEnv = RpcEnv.create(SYSTEM_NAME, host, port, conf)
    val endpoint_name = ENDPOINT_NAME_MASTER + s"-$host:$port"
    //    val endpoint_name = if (masterHostOrNull == null) ENDPOINT_NAME_MASTER + s"-$host:$port" else ENDPOINT_NAME_STANDBY + s"-$host:$port"
    val masterEndpoint = rpcEnv.setupEndpoint(endpoint_name, new Master(rpcEnv, rpcEnv.address, conf))
    //    val masterEndpoint = rpcEnv.setupEndpoint(endpoint_name, new Master(rpcEnv, rpcEnv.address, masterHostOrNull, masterPortOrZero, conf))
    val portsResponse = masterEndpoint.askSync[BoundPortsResponse](BoundPortsRequest)
    (rpcEnv, portsResponse.webUIPort, portsResponse.restPort)
  }
}
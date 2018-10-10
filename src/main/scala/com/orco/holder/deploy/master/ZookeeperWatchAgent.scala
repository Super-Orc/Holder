package com.orco.holder.deploy.master

import com.orco.holder.HolderConf
import com.orco.holder.deploy.HolderCuratorUtil
import com.orco.holder.internal.Logging
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.cache.PathChildrenCache

private[master] class ZookeeperWatchAgent(conf: HolderConf) extends Logging {

    private val WORKING_DIR = conf.get("holder.deploy.zookeeper.dir", "/test")+ "/master_status"
//  private val WORKING_DIR = conf.get("holder.deploy.zookeeper.dir", "/test") + "/node_watch"
  private val zk: CuratorFramework = HolderCuratorUtil.zk
//  private val zk: CuratorFramework = HolderCuratorUtil.newClient(conf)

  HolderCuratorUtil.mkdir(zk, WORKING_DIR)

  //  def getNodeCache: NodeCache = {
  //    val nodeCache = new NodeCache(zk, WORKING_DIR)
  //    nodeCache.start(true)
  //    nodeCache
  //  }

  def getPathChildrenCache: PathChildrenCache = {
    val pathChildrenCache = new PathChildrenCache(zk, WORKING_DIR, true)
    pathChildrenCache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT)
    pathChildrenCache
  }

}


//这些之前是放在 master 里，用来 watch zk 节点的，后来感觉不需要，直接在选举那里就可以操作节点，所以 watch 就移除了
//private val zkWatchThread = ThreadUtils.newDaemonSingleThreadExecutor("zookeeper-watch-thread-pool")
//private var reStartChildFutures: JFuture[_] = _
//private val reStarChildScheduler = ThreadUtils.newDaemonSingleThreadScheduledExecutor("start-child")
//private var zookeeperWatch: PathChildrenCache = _
//private var reStartChildTimer: Option[JScheduledFuture[_]] = None
//private val childListenScheduler = ThreadUtils.newDaemonSingleThreadScheduledExecutor("restart-child")
///**
//  * 保存的是监控节点
//  */
//private val childMap: mutable.Map[String, String] = new mutable.HashMap[String, String]
//private val childMapToStart: mutable.Map[String, String] = new mutable.HashMap[String, String]
//



//zookeeperWatch = zkFactory.createZkWatchAgent()

//    zkWatchThread.submit(new Runnable {
//      override def run(): Unit = {
//        try {
//          zookeeperWatch.getListenable.addListener(new PathChildrenCacheListener {
//            override def childEvent(client: CuratorFramework, event: PathChildrenCacheEvent): Unit = {
//              event.getType match {
//
//                case PathChildrenCacheEvent.Type.CHILD_ADDED =>
//                  println("事件类型：CHILD_ADDED；操作节点：" + event.getData.getPath)
//                  childMap(event.getData.getPath) = persistenceEngine.readFromPath(event.getData.getPath).get
//
//                  if (childMapToStart.contains(event.getData.getPath)) {
//                    if (reStartChildFutures != null) {
//                      reStartChildFutures.cancel(true)
//                    }
//                    childMapToStart -= event.getData.getPath
//                  }
//                  reStartChildTimer match {
//                    case None =>
//                      reStartChildTimer = Some(childListenScheduler.scheduleAtFixedRate(
//                        new Runnable {
//                      override def run(): Unit = Utils.tryLogNonFatalError {
//                  if (childMapToStart.nonEmpty) {}
//                  }
//                  }, 1, 30, TimeUnit.SECONDS))
//                case Some(_) =>
//                  logInfo("Not spawning another attempt to register with the master, since there is an" +
//                    " attempt scheduled already.")
//                  }
//
//                /**
//                  * 1、节点 remove，非 master，则 master 去启动节点
//                  * 2、节点 remove，是 master，则 standby 节点去启动节点，
//                  * 3、
//                  */
//                case PathChildrenCacheEvent.Type.CHILD_REMOVED =>
//                  println("事件类型：CHILD_REMOVED；操作节点：" + event.getData.getPath)
//                  childMapToStart(event.getData.getPath) = persistenceEngine.readFromByte(event.getData).get
//                  //                  childMapToStart(event.getData.getPath) = persistenceEngine.readFromPath(event.getData.getPath).get
//
//                  if (isMaster) {
//                    // master 启动一个线程去重启 child
//                    //成功重启后，master 在 child_add 那里改变状态
//
//                    reStartChildFutures = reStarChildScheduler.submit(new Runnable {
//                      override def run(): Unit = {
//                        try {
//                          logInfo(s"restart child:${event.getData.getPath}...")
//                          //                                                    restartChild(event.getData.getPath,)
//                        } catch {
//                          case ie: InterruptedException => // Cancelled
//                          case NonFatal(e) => logWarning(s"Failed to connect to restart child : ${event.getData.getPath}", e)
//                        }
//                      }
//                    })
//                  }
//                  childMap -= event.getData.getPath
//                case _ =>
//              }
//            }
//          })
//        } catch {
//          case ie: InterruptedException => // Cancelled
//          case NonFatal(e) => logWarning(s"Failed to connect to master $masterHost:$masterPort", e)
//        }
//      }
//    })
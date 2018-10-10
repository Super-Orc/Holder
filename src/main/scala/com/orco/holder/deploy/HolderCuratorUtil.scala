package com.orco.holder.deploy

import java.util

import com.orco.holder.{HolderConf, HolderException}
import com.orco.holder.internal.Logging
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.zookeeper.KeeperException
import scala.collection.JavaConversions._
import scala.collection.mutable

private[holder] object HolderCuratorUtil extends Logging {

  private val ZK_CONNECTION_TIMEOUT_MILLIS = 15000
  private val ZK_SESSION_TIMEOUT_MILLIS = 60000
  private val RETRY_WAIT_MILLIS = 5000
  private val MAX_RECONNECT_ATTEMPTS = 3

  var zk: CuratorFramework = newClient()
  private var WORKING_DIR: String = _

  def newClient(conf: HolderConf = new HolderConf(),
                zkUrlConf: String = "holder.deploy.zookeeper.url"): CuratorFramework = {
    WORKING_DIR = conf.get("holder.deploy.zookeeper.dir", "/holder") + "/master_status"
    val ZK_URL = conf.get(zkUrlConf)
    zk = CuratorFrameworkFactory.newClient(ZK_URL,
      ZK_SESSION_TIMEOUT_MILLIS, ZK_CONNECTION_TIMEOUT_MILLIS,
      new ExponentialBackoffRetry(RETRY_WAIT_MILLIS, MAX_RECONNECT_ATTEMPTS))
    zk.start()
    //todo 这行是为了防止新机器上没有 dir，会空指针，可以优雅一些？
    mkdir(zk, WORKING_DIR)
    zk
  }


  def mkdir(zk: CuratorFramework, path: String) {
    if (zk.checkExists().forPath(path) == null) {
      try {
        zk.create().creatingParentsIfNeeded().forPath(path)
      } catch {
        case nodeExist: KeeperException.NodeExistsException =>
        // do nothing, ignore node existing exception.
        case e: Exception => throw e
      }
    }
  }

  def deleteRecursive(zk: CuratorFramework, path: String) {
    if (zk.checkExists().forPath(path) != null) {
      for (child <- zk.getChildren.forPath(path)) {
        zk.delete().forPath(path + "/" + child)
      }
      //      zk.delete().forPath(path)
    }
  }

  /**
    * master 节点是否存在
    *
    */
  def checkExists(path: String): (String, Int) = {
    mkdir(zk, WORKING_DIR)
    var host: String = null
    var port: Int = 0
    val p: mutable.Seq[String] = zk.getChildren.forPath(WORKING_DIR).filter(_.startsWith(path))
    if (p.size == 1) {
      host = p.head.split("-")(2)
      port = p.head.split("-")(3).toInt
    } else {
      if (p.size > 1)
        throw new HolderException("more than one master found from zk")
    }
    (host, port)
  }

  def getNextPort: Int = {
    val list: util.List[String] = zk.getChildren.forPath(WORKING_DIR)
    if (list.size() > 0)
      list.map(port => port.substring(port.length - 5).toInt).max + 1
    else 15000
  }
}

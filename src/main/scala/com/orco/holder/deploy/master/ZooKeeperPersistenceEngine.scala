package com.orco.holder.deploy.master


import java.nio.ByteBuffer
import java.util

import com.orco.holder.HolderConf
import com.orco.holder.deploy.HolderCuratorUtil
import com.orco.holder.internal.Logging
import com.orco.holder.serializer.Serializer
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.imps.CuratorFrameworkState
import org.apache.curator.framework.recipes.cache.ChildData
import org.apache.zookeeper.CreateMode

import scala.collection.JavaConversions._
import scala.reflect.ClassTag


private[master] class ZooKeeperPersistenceEngine(conf: HolderConf, val serializer: Serializer)
  extends PersistenceEngine
    with Logging {

  private val ZK_DIR = conf.get("holder.deploy.zookeeper.dir", "/holder")
  private val WORKING_DIR: String = s"$ZK_DIR/master_status"
  private val zk: CuratorFramework = HolderCuratorUtil.zk
  //  private val zk: CuratorFramework = HolderCuratorUtil.newClient(conf)

  HolderCuratorUtil.mkdir(zk, WORKING_DIR)

  override def persist(name: String, obj: Object): Unit = {
    if (zk.checkExists().forPath(WORKING_DIR + "/" + name) == null) {
      serializeIntoFile(WORKING_DIR + "/" + name, obj)
    } else {
      logInfo(s"ZkPath exists,can't persist this path ${WORKING_DIR + "/" + name}")
    }
  }

  override def unpersist(name: String): Unit = {
    if (zk.getState == CuratorFrameworkState.STARTED) {

      if (zk.checkExists().forPath(WORKING_DIR + "/" + name) != null) {
        zk.delete().forPath(WORKING_DIR + "/" + name)
      } else {
        logInfo(s"ZkPath is not exists,can't delete this path ${WORKING_DIR + "/" + name}")
      }
    }
  }

  //我自己写的，忘了有什么意义，暂时废弃
  override def ensureSimilarIp(host: String, port: Int): Unit = {
    unpersist(zk.getChildren.forPath(WORKING_DIR).find(_.endsWith(host + "-" + port)).getOrElse("noSuchPath"))
  }

  /**
    * master 节点是否存在
    *
    * @return true : master 存在
    */
  override def checkExists(path: String): Boolean = {
    zk.getChildren.forPath(WORKING_DIR).exists(_.startsWith(path))
  }

  override def getNextPort: Int = {
    val list: util.List[String] = zk.getChildren.forPath(WORKING_DIR)
    list.map(port => port.substring(port.length - 4).toInt).max + 1
  }

  override def read[T: ClassTag](prefix: String): Seq[T] = {
    zk.getChildren.forPath(WORKING_DIR)
      .filter(_.startsWith(prefix)).flatMap(name => deserializeFromFile[T](name))
  }

  override def readFromPath[T: ClassTag](path: String): Option[T] = deserializeFromFilePath(path)

  override def close() {
    zk.close()
  }

  override def cleanZK(): Unit = {
    HolderCuratorUtil.deleteRecursive(zk, ZK_DIR + "/leader_election")
    HolderCuratorUtil.deleteRecursive(zk, ZK_DIR + "/master_status")
  }

  private def serializeIntoFile(path: String, value: AnyRef) {
    val serialized: ByteBuffer = serializer.newInstance().serialize(value)
    val bytes = new Array[Byte](serialized.remaining())
    serialized.get(bytes)
    zk.create().withMode(CreateMode.EPHEMERAL).forPath(path, bytes)
  }

  private def deserializeFromFile[T](filename: String)(implicit m: ClassTag[T]): Option[T] = {
    val fileData = zk.getData.forPath(WORKING_DIR + "/" + filename)
    try {
      Some(serializer.newInstance().deserialize[T](ByteBuffer.wrap(fileData)))
    } catch {
      case e: Exception =>
        logWarning("Exception while reading persisted file, deleting", e)
        zk.delete().forPath(WORKING_DIR + "/" + filename)
        None
    }
  }

  private def deserializeFromFilePath[T](path: String)(implicit m: ClassTag[T]): Option[T] = {
    val fileData: Array[Byte] = zk.getData.forPath(path)
    try {
      Some(serializer.newInstance().deserialize[T](ByteBuffer.wrap(fileData)))
    } catch {
      case e: Exception =>
        logWarning("Exception while reading persisted file, deleting", e)
        zk.delete().forPath(path)
        None
    }
  }

  override def readFromByte[T: ClassTag](data: ChildData): Option[T] = {
    try {
      Some(serializer.newInstance().deserialize[T](ByteBuffer.wrap(data.getData)))
    } catch {
      case e: Exception =>
        logWarning("Exception while reading persisted file, deleting", e)
        zk.delete().forPath(data.getPath)
        None
    }
  }
}

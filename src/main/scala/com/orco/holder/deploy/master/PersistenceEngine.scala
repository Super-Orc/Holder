package com.orco.holder.deploy.master

import com.orco.holder.executor.ContainerInfo
import org.apache.curator.framework.recipes.cache.ChildData

import scala.reflect.ClassTag


/**
  * Allows Master to persist any state that is necessary in order to recover from a failure.
  * The following semantics are required:
  *   - addApplication and addWorker are called before completing registration of a new app/worker.
  *   - removeApplication and removeWorker are called at any time.
  * Given these two requirements, we will have all apps and workers persisted, but
  * we might not have yet deleted apps or workers that finished (so their liveness must be verified
  * during recovery).
  *
  * The implementation of this trait defines how name-object pairs are stored or retrieved.
  */
abstract class PersistenceEngine {

  /**
    * Defines how the object is serialized and persisted. Implementation will
    * depend on the store used.
    */
  def persist(name: String, obj: Object): Unit

  /**
    * Defines how the object referred by its name is removed from the store.
    */
  def unpersist(name: String): Unit

  def checkExists(path: String): Boolean

  def cleanZK()

  def getNextPort: Int

  /**
    * Gives all objects, matching a prefix. This defines how objects are
    * read/deserialized back.
    */
  def read[T: ClassTag](prefix: String): Seq[T]

  def readFromPath[T: ClassTag](path: String): Option[T]

  def readFromByte[T: ClassTag](data: ChildData): Option[T]


  def ensureSimilarIp(ip: String, host: Int): Unit

  final def addMaster(master: MasterInfo): Unit = {
    //    ensureSimilarIp(master.host, master.port)
    persist(master.id, master)
  }

  final def removeMaster(masterId: String): Unit = {
    unpersist(masterId)
  }

  final def addMasterStandBy(masterStandBy: MasterStandByInfo): Unit = {
    //    ensureSimilarIp(masterStandBy.host, masterStandBy.port)
    persist(masterStandBy.id, masterStandBy)
  }

  final def removeMasterStandBy(masterStandById: String): Unit = {
    unpersist(masterStandById)
  }

  final def addContainer(container: ContainerInfo): Unit = {
//    assert(bean.executor.name != null, "Container name can not be null")
    persist(container.id, container)
  }

  final def removeContainer(name: String): Unit = {
    unpersist(name)
  }

  def close() {}
}
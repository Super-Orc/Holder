package com.orco.holder.deploy.master


import com.orco.holder.HolderConf
import com.orco.holder.serializer.Serializer
import org.apache.curator.framework.recipes.cache.PathChildrenCache

/**
  * ::DeveloperApi::
  *
  * Implementation of this class can be plugged in as recovery mode alternative for holder's
  * Standalone mode.
  *
  */
abstract class StandaloneRecoveryModeFactory(conf: HolderConf, serializer: Serializer) {

  /**
    * PersistenceEngine defines how the persistent data(Information about worker, driver etc..)
    * is handled for recovery.
    *
    */
  def createPersistenceEngine(): PersistenceEngine

  /**
    * Create an instance of LeaderAgent that decides who gets elected as master.
    */
  def createLeaderElectionAgent(master: LeaderElectable): LeaderElectionAgent

  def createZkWatchAgent(): PathChildrenCache
}


private[master] class ZooKeeperRecoveryModeFactory(conf: HolderConf, serializer: Serializer)
  extends StandaloneRecoveryModeFactory(conf, serializer) {

  def createPersistenceEngine(): PersistenceEngine = {
    new ZooKeeperPersistenceEngine(conf, serializer)
  }

  def createLeaderElectionAgent(master: LeaderElectable): LeaderElectionAgent = {
    new ZooKeeperLeaderElectionAgent(master, conf)
  }

  override def createZkWatchAgent(): PathChildrenCache = {
    new ZookeeperWatchAgent(conf).getPathChildrenCache
  }
}

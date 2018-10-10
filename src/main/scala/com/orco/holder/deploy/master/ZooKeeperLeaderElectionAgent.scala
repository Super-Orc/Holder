package com.orco.holder.deploy.master


import com.orco.holder.HolderConf
import com.orco.holder.deploy.HolderCuratorUtil
import com.orco.holder.internal.Logging
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.leader.{LeaderLatch, LeaderLatchListener}

private[master] class ZooKeeperLeaderElectionAgent(val masterInstance: LeaderElectable,
                                                   conf: HolderConf) extends LeaderLatchListener with LeaderElectionAgent with Logging {

  val WORKING_DIR: String = conf.get("holder.deploy.zookeeper.dir", "/holder") + "/leader_election"

  private var zk: CuratorFramework = _
  private var leaderLatch: LeaderLatch = _
  private var status = LeadershipStatus.NOT_LEADER

  start()

  private def start() {
    logInfo("Starting ZooKeeper LeaderElection agent")
    zk = HolderCuratorUtil.zk
    //    zk = HolderCuratorUtil.newClient(conf)
    leaderLatch = new LeaderLatch(zk, WORKING_DIR)
    leaderLatch.addListener(this)
    leaderLatch.start()
  }

  override def stop() {
    leaderLatch.close()
//    if (zk.getState != CuratorFrameworkState.STOPPED)
      zk.close()
  }

  override def isLeader() {
    synchronized {
      // could have lost leadership by now.
      if (!leaderLatch.hasLeadership) {
        return
      }

      logInfo("We have gained leadership")
      updateLeadershipStatus(true)
    }
  }

  override def notLeader() {
    synchronized {
      // could have gained leadership by now.
      if (leaderLatch.hasLeadership) {
        return
      }

      logInfo("We have lost leadership")
      updateLeadershipStatus(false)
    }
  }

  private def updateLeadershipStatus(isLeader: Boolean) {
    if (isLeader && status == LeadershipStatus.NOT_LEADER) {
      status = LeadershipStatus.LEADER
      logInfo(s"i am $status")
      masterInstance.electedLeader()
    } else if (!isLeader && status == LeadershipStatus.LEADER) {
      status = LeadershipStatus.NOT_LEADER
      logInfo(s"i am $status")
      masterInstance.revokedLeadership()
    }
  }

  private object LeadershipStatus extends Enumeration {
    type LeadershipStatus = Value
    val LEADER, NOT_LEADER = Value
  }

}

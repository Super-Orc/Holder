package com.orco.holder.deploy.master

trait LeaderElectionAgent {
  val masterInstance: LeaderElectable
  def stop() {} // to avoid noops in implementations.
}
trait LeaderElectable {
  def electedLeader(): Unit
  def revokedLeadership(): Unit
}
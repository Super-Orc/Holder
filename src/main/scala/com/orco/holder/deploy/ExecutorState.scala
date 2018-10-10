package com.orco.holder.deploy

private[orco] object ExecutorState extends Enumeration {

  val LAUNCHING, RUNNING, KILLED, FAILED, LOST, EXITED = Value

  type ExecutorState = Value

  def isFinished(state: ExecutorState): Boolean = Seq(FAILED, LOST, EXITED).contains(state)
//  def isFinished(state: ExecutorState): Boolean = Seq(KILLED, FAILED, LOST, EXITED).contains(state)
}

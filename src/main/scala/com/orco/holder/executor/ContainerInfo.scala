package com.orco.holder.executor

import java.util.Date

import com.orco.holder.deploy.worker.ExecutorRunner


private[orco] class ContainerInfo(var id: String,
                                  var nodeId: String,
                                  val name: String,
                                  val fileName: String,
                                  val startTime: Long,
                                  var message: String,
                                  var executor: ExecutorRunner,
                                  //                                  var containerRef: RpcEndpointRef,
                                  var webUiUrl: String,
                                  var memory: Int,
                                  var masterWebUrl:String=null,
                                  val submitDate: Date = new Date(),
                                  var state: String = "RUNNING",
                                  val user: String = System.getProperty("user.name", "<unknown>"),
                                  var endTime: Long = 0L) extends Serializable {
  //  @transient var endTime: Long = 0L

  def markFinished() {
    state = "FINISHED"
    endTime = System.currentTimeMillis()
    executor.close()
    executor = null
    //    containerRef = null
  }

  def duration: Long = {
    if (endTime != 0) {
      endTime - startTime
    } else {
      System.currentTimeMillis() - startTime
    }
  }

  def isLocal: Boolean = id.split("-")(2) == nodeId.split("-")(2)


  override def toString = s"ContainerInfo($name, $nodeId)"
}
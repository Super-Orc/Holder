package com.orco.holder.deploy

import com.orco.holder.deploy.DeployMessages.MasterStateResponse
import com.orco.holder.executor.ContainerInfo
import org.json4s.JsonAST.JObject
import org.json4s.JsonDSL._
private[deploy] object JsonProtocol {

  def writeApplicationInfo(obj: ContainerInfo): JObject = {
    println(111111111)
    ("id" -> obj.id) ~
      ("starttime" -> obj.startTime) ~
      ("name" -> obj.name) ~
      ("cores" -> 123) ~
      ("memoryperslave" -> obj.executor.memory) ~
      ("submitdate" -> obj.submitDate.toString) ~
      ("duration" -> obj.duration)
  }

  def writeMasterState(obj: MasterStateResponse): JObject = {
    ("url" -> obj.uri) ~
      ("activeapps" -> obj.activeApps.toList.map(writeApplicationInfo)) ~
      ("status" -> "JsonProtocol.status")
  }

}

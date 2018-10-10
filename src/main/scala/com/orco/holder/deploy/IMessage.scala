package com.orco.holder.deploy

sealed trait IMessage extends Serializable

private[deploy] object IMessage {

  case object MyRequest

  case class MyResponse(name: String, age: Int)

}

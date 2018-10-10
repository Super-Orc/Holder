package com.orco.holder.rpc

/**
  * Address for an RPC environment, with hostname and port.
  */
private[orco] case class RpcAddress(host: String, port: Int) {

  def hostPort: String = host + ":" + port

  /** Returns a string in the form of "holder://host:port". */
//  def toSparkURL: String = "holder://" + hostPort

  override def toString: String = hostPort
}

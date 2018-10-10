package com.orco.holder.util

import com.orco.holder.HolderConf
import com.orco.holder.rpc.RpcTimeout


private[holder] object RpcUtils {


  /** Returns the configured number of times to retry connecting */
  def numRetries(conf: HolderConf): Int = {
    conf.getInt("holder.rpc.numRetries", 3)
  }

  /** Returns the configured number of milliseconds to wait on each retry */
  def retryWaitMs(conf: HolderConf): Long = {
    conf.getTimeAsMs("holder.rpc.retry.wait", "3s")
  }

  /** Returns the default Spark timeout to use for RPC ask operations. */
  def askRpcTimeout(conf: HolderConf): RpcTimeout = {
    RpcTimeout(conf, Seq("holder.rpc.askTimeout", "holder.network.timeout"), "60s")
  }

  /** Returns the default Spark timeout to use for RPC remote endpoint lookup. */
  def lookupRpcTimeout(conf: HolderConf): RpcTimeout = {
    RpcTimeout(conf, Seq("holder.rpc.lookupTimeout", "holder.network.timeout"), "120s")
  }
}
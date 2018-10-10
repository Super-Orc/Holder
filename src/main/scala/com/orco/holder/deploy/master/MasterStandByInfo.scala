package com.orco.holder.deploy.master

import com.orco.holder.rpc.RpcEndpointRef


private[deploy] class MasterStandByInfo(val id: String,
                                        val host: String,
                                        val port: Int,
                                        val name: String,
                                        val standByRef: RpcEndpointRef,
                                        val webUiAddress: String
                                       )
  extends Serializable {

  override def toString = s"MasterStandByInfo(id=$id, host=$host, port=$port, name=$name)"
}


package com.orco.holder.deploy.master

import com.orco.holder.rpc.RpcEndpointRef


private[deploy] class MasterInfo(val id: String,
                                 val host: String,
                                 val port: Int,
                                 val name: String,
                                 val masterRef: RpcEndpointRef
                                )
  extends Serializable {

  override def toString = s"MasterInfo(id=$id, host=$host, port=$port, name=$name)"
}

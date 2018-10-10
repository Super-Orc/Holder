package com.orco.holder.rpc

import com.orco.holder.HolderException

private[orco] class RpcEndpointNotFoundException(uri: String)
  extends HolderException(s"Cannot find endpoint: $uri")

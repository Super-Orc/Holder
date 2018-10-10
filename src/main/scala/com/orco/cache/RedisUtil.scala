package com.orco.cache

import redis.clients.jedis.{JedisPool, JedisPoolConfig}

object RedisUtil extends Serializable {

  lazy val config: JedisPoolConfig = {
    val config = new JedisPoolConfig
    config.setMaxTotal(5)
    config.setMaxIdle(3)
    config.setMaxWaitMillis(20000)
    config.setTestOnBorrow(false)
    config.setTestOnReturn(false)
    config
  }
  lazy val pool = new JedisPool(config,
    "116.62.190.105",
    6380,
    60000,
    "rlhz!rc69", 2)

  lazy val hook: Thread = new Thread {
    override def run(): Unit = {
      println("Execute hook thread: " + this)
      pool.destroy()
    }
  }
  sys.addShutdownHook(hook.run())
}
package com.orco.job

import com.orco.cache.RedisUtil
import redis.clients.jedis.Jedis

import scala.util.parsing.json.JSONObject

object JobEvents3 {
  def main(args: Array[String]): Unit = {

    val map =
      Map("name" -> "quartz3",
        "classPackage" -> "com.quartz.core.TestJar",
        "method" -> "start3")

    val jedis: Jedis = RedisUtil.pool.getResource
    jedis.publish("orco:start", JSONObject(map).toString())
    //        jedis.publish("orco:stop", SUtil.beString(jar))
    jedis.close()


  }
}

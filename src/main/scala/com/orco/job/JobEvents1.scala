package com.orco.job

import com.orco.cache.RedisUtil
import redis.clients.jedis.Jedis

import scala.util.parsing.json.JSONObject

object JobEvents1 {



  def main(args: Array[String]): Unit = {

    val map =
      Map("name" -> "quartz1",
        "classPackage" -> "com.quartz.core.TestJar",
        "method" -> "start1")

    val jedis: Jedis = RedisUtil.pool.getResource
    jedis.publish("orco:start", JSONObject(map).toString())
    //        jedis.publish("orco:stop", SUtil.beString(jar))
    jedis.close()


  }
}

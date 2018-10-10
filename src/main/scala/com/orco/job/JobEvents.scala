package com.orco.job

import com.orco.cache.RedisUtil
import redis.clients.jedis.Jedis

import scala.util.parsing.json.JSONObject

object JobEvents {


  val path = "/tmp/orco-local-dir/jars/"

  def main(args: Array[String]): Unit = {

    val map =
      Map("name" -> "quartz",
        "classPackage" -> "com.loktar.quartz.QuartzManager",
        "method" -> "onStart")

    val jedis: Jedis = RedisUtil.pool.getResource
    jedis.publish("orco:start", JSONObject(map).toString())
    //        jedis.publish("orco:stop", SUtil.beString(jar))
    jedis.close()


  }
}

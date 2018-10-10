package com.orco.job

import com.orco.cache.RedisUtil
import redis.clients.jedis.Jedis

import scala.util.parsing.json.JSONObject

object JobEvents2 {
  def main(args: Array[String]): Unit = {
    val jedis: Jedis = RedisUtil.pool.getResource

    List(1,2,3).foreach(a=>{

    val map =
      Map("name" -> s"quartz$a",
        "classPackage" -> "com.quartz.core.TestJar",
        "method" -> s"start$a")

    jedis.publish("orco:start", JSONObject(map).toString())
    })
    //        jedis.publish("orco:stop", SUtil.beString(jar))
    jedis.close()


  }
}

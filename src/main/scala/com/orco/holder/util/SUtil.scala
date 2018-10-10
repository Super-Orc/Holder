package com.orco.holder.util

import java.util.concurrent.ConcurrentHashMap

import com.alibaba.fastjson.{JSON, JSONObject}
import com.orco.holder.internal.Logging

import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

object SUtil extends Logging {

  def tryCatch(block: => Any, any: Any = "default"): Any =
    try
      block
    catch {
      case NonFatal(t) => logError(s"tryCatch exception in thread ${Thread.currentThread().getName},$any", t)
    }

  /**
    * if promise return null,the result of func safeTry will be None
    */
  def safeTry[T](premise: => T, any: Any = null): Option[T] =
    Try(premise) match {
      case Success(success) =>
        Option(success)
      case Failure(failure) => logError(s"safeTry error,$any", failure)
        None
    }

  def beString(any: AnyRef): String = JSON.toJSONString(any, false)

  def beObject[T](str: String, clazz: Class[T]): T = JSON.parseObject(str, clazz)

  def beObject(str: String): JSONObject = JSON.parseObject(str)


  def main(args: Array[String]): Unit = {
    val a = new ConcurrentHashMap[String,String]()
    a.put("1","1")
    a.put("1","2")
    println(a)
  }
}

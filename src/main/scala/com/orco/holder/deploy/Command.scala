package com.orco.holder.deploy

import scala.collection.Map

private[orco] case class Command(
                                  mainClass: String,
                                  arguments: Seq[String],
                                  environment: Map[String, String],
                                  classPathEntries: Seq[String],
                                  libraryPathEntries: Seq[String],
                                  javaOpts: Seq[String]) {
  def msg: String = {
    var message: String = ""
    var argv = arguments.toList
    while (argv.nonEmpty) {
      argv match {
        case ("--message") :: value :: tail =>
          message = value
          argv = tail
        case _ =>
          argv =List()
      }
    }
    message
  }

}

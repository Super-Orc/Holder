package com.orco.holder.util


/**
  * An extractor object for parsing strings into integers.
  */
private[holder] object IntParam {
  def unapply(str: String): Option[Int] = {
    try {
      Some(str.toInt)
    } catch {
      case e: NumberFormatException => None
    }
  }
}

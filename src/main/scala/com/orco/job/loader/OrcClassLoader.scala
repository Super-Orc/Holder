package com.orco.job.loader

import java.net.{JarURLConnection, URL, URLClassLoader, URLConnection}
import java.util.concurrent.ConcurrentHashMap

import com.orco.holder.HolderException
import com.orco.holder.internal.Logging
import com.orco.holder.util.SUtil

object OrcoClassLoader extends Logging {

  def main(args: Array[String]): Unit = {
    val path = "/Users/orco/project/se_project/JustTest/QuartzTest/target"
    1 to 20 foreach { _ =>
      SUtil.tryCatch {
        OrcoClassLoader.loadJar(path, "QuartzTest-1.0-SNAPSHOT.jar")
        val clazz: Class[_] = OrcoClassLoader.loadClass("QuartzTest-1.0-SNAPSHOT.jar", "com.com.dynjar.TestDynJar")
        val method = clazz.getDeclaredMethod("doSth")
        method.invoke(clazz.newInstance())
        Thread.sleep(3000)
        OrcoClassLoader.unLoadJar("QuartzTest-1.0-SNAPSHOT.jar")
      }
    }

  }

  /**
    * 根据表中配置，重新加载 jar，运行 class
    */
//  def onStart(db: TxJar): Unit =
//  //  def onStart(jarPath: String, jarName: String, classPackage: String, methods: String): Unit =
//    SUtil.tryCatch {
//      OrcoClassLoader.loadJar(db.getJarPath, db.getJarName)
//      val clazz = OrcoClassLoader.loadClass(db.getJarName, db.getClassPackage)
//      val method = clazz.getDeclaredMethod(db.getMethod)
//      method.setAccessible(true)
//      method.invoke(clazz.newInstance())
//      //      OrcoClassLoader.unLoadJar("QuartzTest-1.0-SNAPSHOT.jar")
//    }

  val loader = new ConcurrentHashMap[String, OrcoURLClassLoader]()

  def loadJar(path: String, jarName: String): Unit = {
    if (loader.putIfAbsent(jarName, new OrcoURLClassLoader(new Array[URL](0), parentClassLoader())) != null) {
      throw new HolderException(s"There is already an jar called $jarName")
    }
    loader.get(jarName).addUrlFile(new URL("jar:file:" + path + "/" + jarName + "!/"))
    logDebug(s"load jar $path/$jarName")
  }

  def unLoadJar(jarName: String): Unit = {
    val classLoader = loader.get(jarName)
    if (classLoader != null) {
      classLoader.unLoadJar()
      loader.remove(jarName)
      logDebug(s"unLoad jar $jarName")
    }
  }

  def loadClass(jarName: String, name: String): Class[_] = {
    val classLoader = loader.get(jarName)
    if (classLoader != null) {
      classLoader.loadClass(name)
    } else {
      null
    }
  }

  def parentClassLoader(): ClassLoader =
    Option(Thread.currentThread().getContextClassLoader).getOrElse(ClassLoader.getSystemClassLoader)
}

class OrcoURLClassLoader(arr: Array[URL], classLoader: ClassLoader) extends URLClassLoader(arr, classLoader) with Logging {

  var cachedConn: JarURLConnection = _

  def addUrlFile(file: URL): Unit = {
    val uc: URLConnection = file.openConnection()
    uc match {
      case connection: JarURLConnection =>
        uc.setUseCaches(true)
        cachedConn = connection
      case _ =>
    }
    addURL(file)
  }

  def unLoadJar(): Unit = {
    val conn: JarURLConnection = cachedConn
    Option(conn) match {
      case Some(jar) =>
        try {
          logDebug(s"Unloading JAR ${jar.getJarFile.getName}")
          jar.getJarFile.close()
        } catch {
          case e: Exception => logError(s"Failed to unload JAR ${jar.getJarFile.getName}", e)
        }
      case None =>
        logError("Unloading JAR failed,JarURLConnection is null")
    }
  }
}
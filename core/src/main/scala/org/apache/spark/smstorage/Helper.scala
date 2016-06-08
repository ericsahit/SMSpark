package org.apache.spark.smstorage

import java.io.IOException
import java.nio.ByteBuffer

import org.apache.spark.SparkConf
import org.apache.spark.network.buffer.{NioManagedBuffer, ManagedBuffer}
import org.apache.spark.smstorage.SBlockId
import org.apache.spark.smstorage.client.io.{LocalBlockOutputStream, LocalBlockInputStream}
import org.apache.spark.storage.BlockNotFoundException
import org.apache.spark.util.Utils

import scala.util.Try

/**
 * Created by Wang Haihua.
 */
object Helper {

  val isDriver = false

  // Create an instance of the class with the given name, possibly initializing it with our conf
  def instantiateClass[T](conf: SparkConf, className: String): T = {
    val cls = Class.forName(className, true, Utils.getContextOrSparkClassLoader)
    // Look for a constructor taking a SparkConf and a boolean isDriver, then one taking just
    // SparkConf, then one taking no arguments
    try {
      cls.getConstructor(classOf[SparkConf], java.lang.Boolean.TYPE)
        .newInstance(conf, new java.lang.Boolean(isDriver))
        .asInstanceOf[T]
    } catch {
      case _: NoSuchMethodException =>
        try {
          cls.getConstructor(classOf[SparkConf]).newInstance(conf).asInstanceOf[T]
        } catch {
          case _: NoSuchMethodException =>
            cls.getConstructor().newInstance().asInstanceOf[T]
        }
    }
  }

  // Create an instance of the class named by the given SparkConf property, or defaultClassName
  // if the property is not set, possibly initializing it with our conf
  def instantiateClassFromConf[T](conf: SparkConf, propertyName: String, defaultClassName: String): T = {
    val className = conf.get(propertyName, defaultClassName)
    instantiateClass[T](conf, className)
  }

  /**
   *
   * @param entry
   * @return
   */
  def getBlock(entry: SBlockEntry): ManagedBuffer = {
    var is: LocalBlockInputStream = null
    if (entry.local) {
      is = LocalBlockInputStream.getLocalInputStream("shmget", entry.entryId, entry.size.toInt)
    } else {//远程Block
      //is =
    }

    assert (is != null)
    try {
      val size = entry.size
      //val bs = new Array[Byte](size.toInt)
      //这里修改为一次性读出Array，返回
      val bs = is.readFully(size.toInt)
      //ByteStreams.readFully(is, bs) //TODO有一次内存拷贝，这里是否可以消除？
      new NioManagedBuffer(ByteBuffer.wrap(bs))
    } catch {
      case ioe: IOException =>
        throw new BlockNotFoundException(entry.userDefinedId)
    } finally {
      is.close()
    }
  }

  def writeBlock(entry: SBlockEntry, data: ManagedBuffer) = {
    var os: LocalBlockOutputStream = null
    var success = true
    val result = Try {
      os = LocalBlockOutputStream.getLocalOutputStream("shmget", entry.entryId, data.nioByteBuffer().limit())
      assert(os != null)
      os.write(data.nioByteBuffer().array())
    }
    os.close()
    result
//
//    try {
//
//    } catch {
//      case ioe: IOException =>
//      case e: Exception =>
//        success = false
//    } finally {
//      if (os != null)
//        os.close()
//    }
//    success
  }

}

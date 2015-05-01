/**
 *
 */
package org.apache.spark.smstorage.worker

import scala.collection.mutable

/**
 * @author hwang
 * 共享存储空间的管理，实现分为Shmget和mmap两种方式
 */
private[spark] class SMManager {
  
  private val entryMap = new mutable.HashMap[String, Entry]
  
  /**
   * 
   */
  def getEntry(reqMemSize: Long): String = {
    ""
  }
  
  /**
   * 
   */
  def releaseEntry(entryStr: String) {
    entryMap.get(entryStr) match {
      case Some(entry) =>
        
      case None =>
        
    }
  }
  
}

class Entry {
  
}
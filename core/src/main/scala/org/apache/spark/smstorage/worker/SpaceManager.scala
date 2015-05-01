/**
 *
 */
package org.apache.spark.smstorage.worker

import org.apache.spark.smstorage.SBlockEntry
import org.apache.spark.util.TimeStampedHashSet
import org.apache.spark.Logging
import org.apache.spark.smstorage.sharedmemory.SMemoryManager

/**
 * @author hwang
 * 管理本节点的存储空间
 * entry对于每一个block都是唯一的，所以可以作为唯一区分的标志
 */
private[spark] class SpaceManager(
    var totalMemory: Long,
    smManager: SMemoryManager) extends Logging {
  
  var usedMemory: Long = 0L
  
  //var totalMemory: Long = 0L
  
  var pendingMemory: Long = 0L
  
  //private val peningEntries = new TimeStampedHashSet[String]
  
  def getAvailableMemory() = {
    totalMemory - usedMemory - pendingMemory
  }
  
  /**
   * 申请空间，如果成功，会锁定这块空间，并且返回共享存储空间的入口
   * TODO: 检查申请的kong
   */
  def checkSpace(reqMemSize: Int) = {
    logInfo(s"ensureFreeSpace($reqMemSize) called with curMem=$usedMemory, maxMem=$totalMemory")
    if (getAvailableMemory() <= reqMemSize) {
      logInfo(s"Will not store the block as it is larger than local memory limit")
      None
    } else {
      Some(smManager.applySpace(reqMemSize));
    }
  }
  
  /**
   * 释放共享存储空间
   */
  def releaseSpace(entryStr: String) {
    smManager.realseSpace(entryStr);
  }
  
}
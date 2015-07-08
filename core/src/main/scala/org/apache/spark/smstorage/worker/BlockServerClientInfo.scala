/**
 *
 */
package org.apache.spark.smstorage.worker

import java.util.{HashMap => JHashMap}
import org.apache.spark.smstorage.BlockServerClientId
import akka.actor.ActorRef
import org.apache.spark.Logging
import org.apache.spark.smstorage.SBlockId
import org.apache.spark.smstorage.SBlockEntry

/**
 * 每一个Executor的info，含有id，最大内存使用，client actor。
 * TODO: ****是否在其中保存Block信息？
 * maxJvmMemSize: Executor的JVM可以达到的最大内存
 */
private[spark] class BlockServerClientInfo(
    val id: BlockServerClientId,
    lastConnectTime: Long,
    val maxJvmMemSize: Long,
    val maxMemSize: Long, 
    val jvmId: Int,
    val clientActor: ActorRef) extends Logging {
  
  private var _lastConnectTime = lastConnectTime
  
  var usedMemory = 0L
  
  var jvmMemory = 0L
  
  private val _blocks = new JHashMap[SBlockId, SBlockEntry]
  
  def blocks = _blocks
  
  //def isMemoryOverLimit = maxMemSize < 
  
  def addBlock(id: SBlockId, entry: SBlockEntry) {
    
    if (!_blocks.containsKey(id)) {
      _blocks.put(id, entry)
      usedMemory += entry.size
    }
    
    updateLastConnectTime()
  }
  
  def removeBlock(id: SBlockId) {
    val block = _blocks.get(id)
    if (block != null) {
      usedMemory -= block.size
      _blocks.remove(id)
    } else {
      None
    }
  }
  
  def getBlock(blockId: SBlockId) = Option(_blocks.get(blockId))
  
  def updateLastConnectTime() {
    _lastConnectTime = System.currentTimeMillis()
  }
  
  /**
   * 更新Block状态
   */
  def updateBlockInfo(blockId: SBlockId) {
    
    updateLastConnectTime()
    
  }
  
  
  
}
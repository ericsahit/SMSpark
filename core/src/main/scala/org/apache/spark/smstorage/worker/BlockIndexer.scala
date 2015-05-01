/**
 *
 */
package org.apache.spark.smstorage.worker

import java.util.HashMap
import org.apache.spark.smstorage.SBlockId
import org.apache.spark.smstorage.SBlockEntry
import java.util.LinkedHashMap
import org.apache.spark.smstorage.SBlockEntry
import org.apache.spark.util.TimeStampedHashMap

/**
 * @author hwang
 *
 */
private[spark] class BlockIndexer {
  
  private val blockList = new TimeStampedHashMap[SBlockId, SBlockEntry]
  
  private var currentMemory = 0L
  
  def getBlock(blockId: SBlockId) = {
    blockList.synchronized {
      blockList.get(blockId)
    }
  }
  
  def getBlockSize(blockId: SBlockId): Option[Long] = {
    blockList.synchronized {
      blockList.get(blockId).map(block => block.size)
    }
  }
  
  def contains(blockId: SBlockId) = {
    blockList.synchronized { blockList.contains(blockId) }
  }
  
  //TODO：生成一个唯一的BlockId
  def addBlock(name: String, entry: SBlockEntry) = {
    blockList.synchronized {
      
      val newBlockId = createNewBlockId()
      blockList.put(newBlockId, entry)
      currentMemory += entry.size
      
      newBlockId
    }
  }
  
  
  def removeBlock(id: SBlockId) = {
    blockList.synchronized {
      val result = blockList.remove(id)
      
      result.map {entry => 
        currentMemory -= entry.size
      }
      
      result

    }
  }
  
  /**
   * TODO：新建一个SBlockId
   */
  def createNewBlockId(): SBlockId = {
    new SBlockId()
  }
  
  
  def clear() {
    blockList.synchronized {
      blockList.clear()
      currentMemory = 0L
    }
  }
}
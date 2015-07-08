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
import org.apache.spark.smstorage.SBlockEntry

/**
 * @author hwang
 *
 * v1: 还需要保存每个client下，Storage的使用情况
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
  //SBlockId使用UserDefinedId作为全局唯一的标识
  def addBlock(entryId: Int, entry: SBlockEntry) = {
    blockList.synchronized {
      
      val newBlockId = createNewBlockId(entryId, entry.userDefinedId)
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
   * 这里生成的SBlockId，localId为空
   * name=entryId, userDefinedId由客户端来指定
   */
  private def createNewBlockId(entryId: Int, userDefinedId: String): SBlockId = {
    new SBlockId(userDefinedId, "", entryId.toString)
  }
  
  
  def clear(f: (SBlockEntry) => Unit) {
    blockList.synchronized {
      blockList.values.foreach(f)
      blockList.clear()
      currentMemory = 0L
    }
  }
}
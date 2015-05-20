/**
 *
 */
package org.apache.spark.smstorage.worker

import scala.collection.mutable
import org.apache.spark.smstorage.BlockServerClientId

/**
 * @author hwang
 * 
 * jvmMemory是Executor当前正在使用的Memory，是实际的计算内存
 * maxMemory是Executor最大可用的存储内存
 * usedMemory是Executor当前使用的存储内存
 * usedMemory+jvmMemory是否大于maxMemory，表明当前应用超出限制
 * 判断当前所有JVMMemorySum+UsedMemorySum是否大于maxMemorySum，或者是否大于安全比例
 * 
 */
private[spark] class ExecutorWatcher (
    spaceManager: SpaceManager,
    indexer: BlockIndexer) {
  
  /**
   * 检查每一个Executor的JVM使用内存
   * 
   */
  def check(clients: mutable.HashMap[BlockServerClientId, BlockServerClientInfo]) {
    var jvmMemorySum: Long = 0L
    
    clients.values.foreach { client =>
      client.jvmMemory = checkJvmMemory(client.jvmId)
      //if (client.jvmMemory)
      jvmMemorySum += client.jvmMemory
    }
    
    /**
     * 如果超出上限，则选举出一个节点，选举出一些Block进行置换，或者远程节点的迁移
     * 这里使用一定的策略，把需要的参数传进去，进行选举。类似于MemoryStore中Block的替换
     */
    if ((jvmMemorySum + spaceManager.usedMemory) >= spaceManager.totalMemory) {
      
    }
    
    
    
    
  }
  
  private def checkJvmMemory(jvmId: Int): Long = {
    
    0L
  }
  
}
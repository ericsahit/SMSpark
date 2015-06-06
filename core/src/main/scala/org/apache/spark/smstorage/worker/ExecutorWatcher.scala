/**
 *
 */
package org.apache.spark.smstorage.worker

import scala.collection.mutable
import org.apache.spark.smstorage.BlockServerClientId
import org.apache.spark.Logging
import java.io.BufferedReader
import java.io.InputStreamReader
import scala.collection.mutable.ArrayBuffer

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
    indexer: BlockIndexer,
    safePercent: Double = 0.9) extends Logging {
  
  /**
   * 检查每一个Executor的JVM使用内存
   * 这里检查了使用的RSS内存，并没有检查JVM内堆内存的使用情况，Young Gen等
   * TODO: 进行调整的时候可以根据JVM内部的使用情况
   */
  def check(clients: mutable.HashMap[BlockServerClientId, BlockServerClientInfo]) {
    
    val currentTime: Long = System.currentTimeMillis()
    var jvmMemorySum: Long = 0L
    
    clients.values.foreach { client =>
      val jvmMemory = ProcfsBasedGetter.getProcessRss(client.jvmId)
      val cid = client.id
      logInfo(s"client $cid current JVM Memory: $jvmMemory")
//      if ((currentTime - startTime) % 10000 == 0) {
//      }
      //if (client.jvmMemory)
      jvmMemorySum += jvmMemory
      client.jvmMemory = jvmMemory
    }
    
    logInfo(s"CurrentTotalMemory($currentTotalMemory), CurrentJvmMemory($jvmMemorySum), TotalMemory($totalMemory).")
    /**
     * 如果超出上限，则选举出一个节点，选举出一些Block进行置换，或者远程节点的迁移
     * 这里使用一定的策略，把需要的参数传进去，进行选举。类似于MemoryStore中Block的替换
     * 可以参考任务调度时候FIFO和FAIR的机制，机制和策略分离
     */
    val currentTotalMemory = jvmMemorySum + spaceManager.usedMemory
    val totalMemory = spaceManager.totalMemory * safePercent
    if (currentTotalMemory >= totalMemory) {
      logInfo(s"CurrentTotalMemory($currentTotalMemory), CurrentJvmMemory($jvmMemorySum) exceed TotalMemory($totalMemory) 90%, need action.")
      //TODO: 替换Block
      doEvictBlock()
    }
    
  }
  
  /**
   * 迁移或者替换Block
   */
  private def doEvictBlock() {
    
  }
  
  private def checkJvmMemory(jvmId: Int): Long = {
    
    val rt = Runtime.getRuntime()
    var reader: BufferedReader = null
    //val procInfo: ProcInfo = new ProcInfo()
    
    try {
      val cmd: Array[String] = Array(
        "/bin/sh",
        "-c",
        "top -b -n 1 | grep " + jvmId
      )
      val execRet = rt.exec(cmd)
      reader = new BufferedReader(new InputStreamReader(execRet.getInputStream()))
      var line: String = null
      var resArr: Array[String] = null
      
      while ((line = reader.readLine()) != null) {
        resArr = line.split("\\s+")
        var idx = 0
        for (info <- resArr if !info.trim().isEmpty()) {//得到空格分隔
          
          if (idx == 5) {//进程使用的物理内存值
            val unit = info.substring(info.length() - 1)
            if (unit.equalsIgnoreCase("g")) {
              
            } else if (unit.equalsIgnoreCase("m")) {
              
            } else {
              
            }
          }
          
          idx += 1
        }
      }
    }
    
    
    0L
  }
  
}


/**
 *
 */
package org.apache.spark.smstorage.master

import org.apache.spark.deploy.master.Master
import scala.collection.mutable
import org.apache.spark.smstorage.SBlockId
import org.apache.spark.smstorage.SBlockEntry
import org.apache.spark.Logging

/**
 * @author hwang
 * Master节点上的BlockServer Actor，负责协调各个节点之间的资源需求
 * 
 * 与Worker节点的通信复用Master与Worker原有的通信，在Master actor中加入BlockServer组件
 * 
 * TODO：删除Block时候，是否观察其他节点正在使用？可以放置到待删除队列中，如果过一段时间没有使用，则真正删除。
 * 
 * TODO：是否使用引用计数，当一个App使用Block时候，更新读取时间，增加引用计数；当程序关闭时候，减少引用计数。
 * 或者可以根据最近读取时间来进行判断
 * 
 */
class BlockServerMaster(val master: Master) extends Logging {
  
  private val sblocks = new mutable.HashMap[String, SBlockEntry]
  
  /**
   * 保存Block的位置，方便查询存取
   */
  private val sblockLocaion = new mutable.HashMap[SBlockId, mutable.HashSet[String]]
  
  /**
   * 得到一个Block，这是一个同步的操作，需要返回Block是否存在
   * 使用场景：
   * 如果改变调度的本地性，那么任务一般都可以调度到Block所在的节点上去执行，
   * 但是当本地性降低（例如Block所在的节点资源被占满）情况下，还可以去远程节点查询，看看是否满足远程的本地性
   * ****TODO：<满足远程的本地性>这里是否通过BlockManagerMaster就可以完成？
   * 就是说，在Executor启动的时候，BlockManager中就可能已经包含一些的Block了
   * 
   * 作用：
   * 当本地的Block不存在时，先去Coordinator节点查询Block是否已经在其他节点存在
   * 
   */
  def getBlock(blockUid: String) = {
    sblocks.get(blockUid)
  }
  
  /**
   * 新增Block，异步操作，不需要返回
   * 
   * 涉及到是否锁定Block
   * 场景：在多个APP之间
   */
  def addBlock(workerId: String, newBlockEntry: SBlockEntry) {
    master.idToWorker.get(workerId) match {
      case Some(workerInfo) =>
        val uid = newBlockEntry.userDefinedId
        //如果对应的Block存在
        sblocks.get(uid) match {
          case Some(oldBlockEntry) =>
            logWarning(s"[SMSpark]Got updateBlock to existed block $uid")
          case None =>
            sblocks += ((uid, newBlockEntry))
            workerInfo.addBlock(newBlockEntry)//添加Worker中的Block索引
        }
      case None =>
        logWarning(s"[SMSpark]Got updateBlock from unregistered worker $workerId.")
    }
  }
  
  def updateWorkerMemory(memoryTotal: Long, memoryUsed: Long) {
    
  }
  
  /**
   * 更新Block
   * Block是不可变的，所以大小不会改变。
   * TODO：适用场景是？
   */
  def updateBlock(workerId: String, newBlockEntry: SBlockEntry) {
    
    master.idToWorker.get(workerId) match {
      case Some(workerInfo) =>
        val uid = newBlockEntry.userDefinedId
        //如果对应的Block存在
        sblocks.get(uid) match {
          case Some(oldBlockEntry) =>
          oldBlockEntry.update(workerId, newBlockEntry)//更新Block索引
          workerInfo.updateBlockInfo(newBlockEntry)//更新Worker中的Block索引
          case None =>
            logWarning(s"[SMSpark]Got updateBlock to unmap block $uid")
        }
      case None =>
        logWarning(s"[SMSpark]Got updateBlock from unregistered worker $workerId.")
    }
  }
  
  /**
   * 更新Block最新的读取时间，异步操作
   * 每次读取Block，更新一下最新的读取时间，方便做替换
   * TODO：使用场景是？
   */
  def updateBlockLastUse(workerId: String, blockId: SBlockId) {
    master.idToWorker.get(workerId) match {
      case Some(workerInfo) =>
        val uid = blockId.userDefinedId
        //如果对应的Block存在
        sblocks.get(uid) match {
          case Some(blockEntry) =>
            blockEntry.updateReadTime()
          case None =>
            logWarning(s"[SMSpark]Got updateBlockLastUse to unmap block $uid")
        }
      case None =>
        logWarning(s"[SMSpark]Got updateBlockLastUse from unregistered worker $workerId.")
    }
  }
  
  /**
   * 删除Block，异步操作
   * 
   * TODO：需要处理是否别的Worker仍然在使用它，从而不实际删除，只是有一个计数减1
   * TODO：使用场景
   */
  def removeBlock(workerId: String, blockId: SBlockId) = {
    master.idToWorker.get(workerId) match {
      case Some(workerInfo) =>
        val uid = blockId.userDefinedId
        //如果对应的Block存在
        sblocks.get(uid) match {
          case Some(blockEntry) =>
            sblocks.remove(uid)
            workerInfo.removeBlock(uid)
          case None =>
            logWarning(s"[SMSpark]Got removeBlock to unmap block $uid")
        }
      case None =>
        logWarning(s"[SMSpark]Got removeBlock from unregistered worker $workerId.")
    }
  }
  
  def removeRDD() = {
    
  }
  
}
/**
 *
 */
package org.apache.spark.smstorage.master

import java.util.{HashMap => JHashMap}
import scala.collection.JavaConversions._

import org.apache.spark.deploy.master.Master
import scala.collection.mutable
import org.apache.spark.smstorage.SBlockId
import org.apache.spark.smstorage.SBlockEntry
import org.apache.spark.Logging
import org.apache.spark.deploy.master.WorkerInfo

/**
 * @author Wang Haihua
 * Master节点上的BlockServer Actor，负责协调各个节点之间的资源需求
 * 
 * 与Worker节点的通信复用Master与Worker原有的通信，在Master actor中加入BlockServer组件
 * 
 * TODO：删除Block时候，是否观察其他节点正在使用？可以放置到待删除队列中，如果过一段时间没有使用，则真正删除。
 * 
 * TODO：是否使用引用计数，当一个App使用Block时候，更新读取时间，增加引用计数；当程序关闭时候，减少引用计数。
 * 或者可以根据最近读取时间来进行判断
 * 
 * 主要用来管理多个Worker节点的资源使用情况，和Block的Location情况
 * 功能：
 * 1）管理元数据：包含RDD所含有的Block状态，分配节点状态
 * 2）选举一个节点，方便Worker节点做数据的迁移
 * 
 * 1) bsWorker注册自己，需要上报自己的节点信息，内存上限信息，这个是异步的。而且启动时候只需要汇报一次
 * 
 * 2) bsWorker定期定期上报自己的内存使用信息，Block更新信息，这个是定期进行汇报，主要是在Block位置进行变更时候，应该不会特别频繁
 * Block更新信息：Block位置信息的变更，新增和删除信息
 * bsWorker信息：内存使用状况，
 * 3) bsWorker上报自己的RDD Block情况，bsMaster保存Block的位置信息
 * 4) 实现基于已缓存的RDD的调度，即新启动的应用程序，发现缓存的数据已经存在，那么就直接得到缓存数据的位置，按照缓存数据的本地性进行调度
 * 这个功能需要存储如下的状态：
 * 每一个Block的位置；
 * TODO: Master
 * 
 */
class BlockServerMaster(val master: Master) extends Logging {
  
  private val sblocks = new mutable.HashMap[String, SBlockEntry]
  
  /**
   * 保存Block的位置，方便查询存取
   * Key: BlockUId 
   * Value: WorkerId
   */
  private val sblockLocaion = new JHashMap[String, mutable.HashSet[String]]
  
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
   * Q: 涉及到是否并发操作Block
   * A: 不会涉及，因为调用还是在Master Actor中，会自动排队单线程
   * 场景：在多个APP之间
   */
  def addBlock(workerId: String, newBlockEntry: SBlockEntry) {
    logDebug(s"[SMSpark]: addBlock to bsMaster. from $workerId, block ${newBlockEntry.userDefinedId}")
    master.idToWorker.get(workerId) match {
      case Some(workerInfo) =>
        val uid = newBlockEntry.userDefinedId
        logDebug(s"[SMSpark]: addBlock find workerInfo, so we will record the block.")
        //如果对应的Block存在
        sblocks.get(uid) match {
          case Some(oldBlockEntry) =>
            logWarning(s"[SMSpark]Got updateBlock to existed block $uid")

            //添加Worker中的Block索引信息
            workerInfo.addBlock(newBlockEntry)

            //添加block的位置信息，Block是否会存在多个位置？在共享存储中应该是不会出现的
            //Block may have multiply location for
            var locations: mutable.HashSet[String] = null
            if (sblockLocaion.containsKey(uid)) {
              locations = sblockLocaion.get(uid)
            } else {
              locations = new mutable.HashSet[String]
              sblockLocaion.put(uid, locations)
            }
            locations.add(workerId)

          case None =>
            //添加Block的索引信息
            sblocks += ((uid, newBlockEntry))
            //添加Worker中的Block索引信息
            workerInfo.addBlock(newBlockEntry)
            
            //添加block的位置信息，Block是否会存在多个位置？在共享存储中应该是不会出现的
            //Block may have multiply location for
            var locations: mutable.HashSet[String] = null
            if (sblockLocaion.containsKey(uid)) {
              locations = sblockLocaion.get(uid)
            } else {
              locations = new mutable.HashSet[String]
              sblockLocaion.put(uid, locations)
            }
            locations.add(workerId)
            
        }
      case None =>
        logWarning(s"[SMSpark]Got updateBlock from unregistered worker $workerId.")
    }
  }
  
  /**
   * memoryTotal目前不会变化，就是节点资源的配置比例
   */
  def updateWorkerMemory(memoryTotal: Long, memoryUsed: Long) {
    
  }
  
  /**
   * 更新Block
   * Block是不可变的，所以大小不会改变。
   * Q：适用场景是？
   * A: 修改Block的存储级别(被替换到磁盘)，数据迁移后新的位置等
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
          
          //添加block的位置信息，Block是否会存在多个位置？在共享存储中应该是不会出现的
          var locations: mutable.HashSet[String] = null
          if (sblockLocaion.containsKey(uid)) {
            locations = sblockLocaion.get(uid)
          } else {
            locations = new mutable.HashSet[String]
            sblockLocaion.put(uid, locations)
          }
          locations.add(workerId)
          
          case None =>
            logWarning(s"[SMSpark]Got updateBlock to not indexed block $uid")
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
  
  def removeRDD() {
    
  }
  
  /**
   * 从Driver端获取BlockServerMaster中的缓存数据情况
   */
  def getLocations(blockId: SBlockId): Seq[String] = {
    logDebug(s"[SMSpark]: getlocations block $blockId")
    if (sblockLocaion.containsKey(blockId.userDefinedId)) {
      logDebug(s"[SMSpark]: Find location for block $blockId: ")
      sblockLocaion.get(blockId.userDefinedId).map { workerId =>
        val host = master.idToWorker.get(workerId).get.host
        logDebug(host)
        host
      }.toSeq
    }
    else
      Seq.empty
  }
  
  def getLocationMultipleSBlockId(sblockIds: Array[SBlockId]) : Seq[Seq[String]] = {

    if (log.isDebugEnabled) {
      sblockIds.map(block => logDebug(s"[SMSpark]: get location for block ${block.userDefinedId}"))

      for (key <-sblockLocaion.keys) {
        var locationsStr = ""

        sblockLocaion.get(key).map(loc => locationsStr += loc + ", ")

        logDebug(s"block $key has location: $locationsStr")
      }
    }



    sblockIds.map(sblockId => getLocations(sblockId))
  }
  
}
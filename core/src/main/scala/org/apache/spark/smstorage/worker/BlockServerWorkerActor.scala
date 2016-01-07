/**
 *
 */
package org.apache.spark.smstorage.worker

import java.util.{HashMap => JHashMap}
import scala.collection.{mutable, immutable}
import scala.collection.JavaConversions._
import scala.concurrent.duration._
import org.apache.spark.Logging
import org.apache.spark.SparkConf
import org.apache.spark.smstorage.BlockServerMessages._
import org.apache.spark.smstorage.BlockServerClientId
import org.apache.spark.smstorage._
import akka.actor.{Actor, ActorRef, Cancellable}
import org.apache.spark.util.{ActorLogReceive, AkkaUtils, Utils, TimeStampedHashMap}
import org.apache.spark.smstorage.sharedmemory.SMemoryManager
import org.apache.spark.deploy.worker.Worker

/**
 * @author hwang
 * worker进程中的BlockServerWorker，负责与所有的本机的Executor中BlockServerWorkerRef进行通信
 * 接收Client进行的请求，对于Akka Actor，会把客户端的请求排队
 * TODO：怎么样保存客户端的信息，以及Block的信息
 * TODO：更新Block，都会更新什么状态（可能与其他节点通信）
 * 需要Client的心跳机制吗？现在看来不需要。
 * TODO: 每一个Block保持一个计数器，如果没有任何程序使用，表明可以删除。如果有计数器不为0，可以被替换或者
 * TODO: Worker节点向Client发送命令
 * TODO：SBlock的id匹配机制，根据RDD血统信息来进行匹配
 * TODO：各个组件的清理工作
 * 
 * 每一个操作现在都需要考虑下列组件：
 * 1) blockIndexer 负责管理本节点的Block
 * 2) clientList 负责管理客户端的列表，其中info中包含属于客户端的Block
 * 3) blocks暂时弃用，使用BlockIndexer替代
 * 4) blockLocation 保存使用block的客户端，get, remove, add, 
 * 5) pendingEntries 保存正在写入的BlockEntry，写入开始和写入结束使用。需要定期清理
 * 6) bsMaster 什么时候向bsMaster节点发送消息
 * 
 * add时候，向bsMaster发送ReqbsMasterAddBlock信息，异步
 * add时候，会由调用者先调用get方法，查看bsMaster是否存在此节点。
 * TODO: get时候，向bsMaster发送增加计数信息，异步向bsMaster节点发送
 * 
 * update 2015.07.08 v1版本：
 * 1.分配存储内存空间修改为，把当前节点的可用存储内存都设置为共享的存储空间。
 * 将存储内存与调度分离，这样每调度一个应用程序Executor，实际上只调度计算内存。
 * 但是上层调度不修改，对调度仍然透明。
 * 例如调度一个1GB的应用程序，实际只使用了400MB的计算内存，600MB的存储内存已经预先在节点进行分配了。
 * 那么，需要保证节点上总存储内存的使用不超过节点存储资源的一定比例，例如memoryCapacity * memoryFraction * saftPoint
 * 
 * 2.元数据管理：
 * 增加相应的元数据管理机制，存储数据替换和多节点的迁移策略所需要的关键信息。
 * 1）数据替换代价
 * 2）迁移目标节点
 * 
 * 3.同时Executor的结束并不会释放缓存数据的共享存储空间
 * 
 * update 2015.07.21 v2版本：
 * 1.增加向bsMaster汇报元数据的管理信息，主要是在写Block时候，或者删除Block时候，对bsMaster发请求进行元数据更新
 * 2.实现基于共享存储RDD的调度
 * ****先不保证计算内存的最大使用？先保证计算内存的初始使用值。那么对于负载，需要使用特定的值。
 * 
 * 目前smspark使用到的参数：
 * spark.smspark.cmemoryFraction 存储内存所占的比例
 *
 *
 */
private[spark]
class BlockServerWorkerActor(conf: SparkConf, worker: Worker)
  extends Actor with ActorLogReceive with Logging {

  /**
   * 共享存储实现的管理组件
   */
  val smManager: SMemoryManager = new SMemoryManager()

  /**
   * 共享存储空间的逻辑管理组件
   * 初始内存，可以设定一个配置的值
   * v1：先设定为集群内存*smemoryFraction，即初始内存就设置相对应的值
   * 初始内存=WorkerMemory * cmemoryFraction * safetyFraction
   * 最大可用内存不随着Executor生命周期的变化而变化
   */
  val spaceManager: SpaceManager = new SpaceManager(getNodeMaxSMemory(conf, worker.memory), smManager)

  /**
   * 索引 Block 的组件
   */
  val blockIndexer: BlockIndexer = new BlockIndexer()

  /**
   * 监控 Executor 内存运行状况的组件
   */
  val executorWatcher: ExecutorWatcher = new ExecutorWatcher(this, spaceManager, blockIndexer)

  /**
   * 保存连接到 BlockServerWorker 的客户端(Executor)，和对应的使用情况
   */
  private val clientList = new mutable.HashMap[BlockServerClientId, BlockServerClientInfo]
  
  /**
   * Block列表，保存Id到Entry信息的映射
   */
  private val blocks = new TimeStampedHashMap[SBlockId, SBlockEntry]
  
  /**
   * 保存BlockId到BlockServerClientId的映射，可能有多个Client在使用。保存Block的位置可以表示有多少人在使用它
   */
  private val blockLocation = new JHashMap[SBlockId, mutable.HashSet[BlockServerClientId]]
  
  //保存被锁定的空间, entryId->SBlockEntry
  //TODO：需要过期清理
  private val pendingEntries = new TimeStampedHashMap[Int, SBlockEntry]
  
  //private val pendingClients = new TimeStampedHashMap[String, BlockServerClientId]

  /**
   * 标志是否有第一个Executor连接到Worker
   * 当第一个Executor连接之后开启ExecutorWatch任务
   */
  private var isFirstExecutorConnected = false

  /**
   * 检查Client是否过期的定时Task
   */
  var timeoutCheckingTask: Cancellable = null
  /**
   * 监控Executor的定时Task
   */
  var execWatchTask: Cancellable = null
  
  private val akkaTimeout = AkkaUtils.askTimeout(conf)
  
  val checkTimeoutInterval = conf.getLong("spark.storage.blockManagerTimeoutIntervalMs", 60000)
  
  val checkExecWatchInterval = conf.getLong("spark.smstorage.executorWatchIntervalMs", 5000)
  
  //////////////////////////////////////////////////////////////////////////////////
  // 统计本节点的一些使用信息
  //////////////////////////////////////////////////////////////////////////////////  
  /**
   * 统计历史访问过数据块的应用Executor个数
   */
  var appTotalCount: Int = 0
  
  override def preStart() {
    logInfo("Starting Spark BlockServerWorker")
    import context.dispatcher
    //定期运行监测client是否失去链接
    //v1&v2：不需要过期检测，数据与Executor不再耦合
//    timeoutCheckingTask = context.system.scheduler.schedule(
//        0.seconds, 
//        checkTimeoutInterval.milliseconds,
//        self,
//        ExpireDeadClient)
        
//   execWatchTask = context.system.scheduler.schedule(
//       1.seconds,
//       checkExecWatchInterval.microseconds,
//       self,
//       CheckExecutorMemory
//       )

    super.preStart()
  }
  
  /**
   * TODO：清理工作
   */
  override def postStop() {
    logInfo("Clean shared memory space when worker closed.")
    def clearEntry(entry: SBlockEntry) = spaceManager.releaseSpace(entry.entryId, entry.size.toInt)
    pendingEntries.values.foreach(clearEntry)
    blockIndexer.clear(clearEntry)
    
    if (worker != null && worker.connected) {
      //TODO: 清理工作
      //worker.master ! 
    }
  }
  
  
  
  override def receiveWithLogging = {
    
    case RegisterBlockServerClient(clientId, maxJvmMemSize, maxMemSize, jvmId, clientActor) =>
      registerClient(clientId, maxJvmMemSize, maxMemSize, jvmId, clientActor)
      sender ! true

    case UnregisterBlockServerClient(clientId) =>
      unregClient(clientId)
      sender ! true
      
    case RequestNewBlock(clientId, name, size) =>
      sender ! reqNewBlock(clientId, name, size)
      
    case WriteBlockResult(clientId, entryId, success) =>
      sender ! writeBlockResult(clientId, entryId, success)
    
    case GetBlock(clientId, blockId) =>
      sender ! getBlock(clientId, blockId)
    
    case ExpireDeadClient =>
      expirtDeadClient()
      
    case CheckExecutorMemory =>
      checkExecutorMemory()
      
    case RemoveBlock(clientId, blockId) =>
      removeBlock(clientId, blockId)
      sender ! true
     
//    case UpdateBlockStatus(clientId, blockId) =>
//      updateBlockStatus(clientId, blockId)
      
    case BlockContains(blockId, local) => //TODO：是否本地？
      sender ! blockIndexer.contains(blockId)

    case ReadSBlock(sblockId, appName) =>
      markReadBlock(sblockId, appName)

    case other =>
      logWarning("unknown blockServerClient message: " + other)
  }
  
  /**
   * 注册客户端
   * 新增：
   * 1）当第一个Executor连接之后开启ExecutorWatch任务
   * 2）向bsMaster更新存储内存的使用信息
   */
  def registerClient(id: BlockServerClientId, maxJvmMemSize: Long, maxMemSize: Long, jvmId: Int, clientActor: ActorRef) = {
    
    if (!isFirstExecutorConnected) {//当第一个Executor连接之后开启ExecutorWatch任务
      isFirstExecutorConnected = true

      import context._
      execWatchTask = this.context.system.scheduler.schedule(
        1.seconds,
        checkExecWatchInterval.milliseconds,
        this.self,
        CheckExecutorMemory
      )
    }
    
    if (!clientList.contains(id)) {
      
      clientList(id) = new BlockServerClientInfo(id, System.currentTimeMillis(), maxJvmMemSize, maxMemSize, jvmId, clientActor)

      spaceManager.totalExecutorMemory += maxJvmMemSize
      //v1: Executor只分配计算内存
      //spaceManager.totalMemory += maxMemSize
      
      //Master这里更新Total的存储内存信息
      if (worker != null)
        worker.sendMasterBSMessage(ReqbsMasterUpdateSMemory(worker.workerId, spaceManager.totalMemory, spaceManager.usedMemory))
      
      logInfo("Registering block server client %s with %s RAM, %s Max JVM RAM, JVMID %d, %s".format(
        id.hostPort, Utils.bytesToString(maxMemSize), Utils.bytesToString(maxJvmMemSize), jvmId, id))
      true  
    } else {
      false
    }
  }
  
  /**
   * 应该保证Block不存在，即先调用contains
   * 新增一个的Block，已经确定不存在block，但是还需要再确定一下
   * 传入userDefinedId
   * TODO: 如何保证一个线程写Block时候，另一个线程能够并发写？
   * 
   * 
   * 1)****在客户端应该是先查询，不存在则新增
   * 1)首先是申请相应空间，锁定空间，返回BlockEntry信息
   * 2)客户端根据申请到的入口，写共享内存
   * 3)客户端返回写成功信息，生成BlockId信息
   * 
   * return:
   * 申请成功：SBLockEntry
   * 申请失败：None
   */
  def reqNewBlock(clientId: BlockServerClientId, userDefinedId: String, size: Long): Option[SBlockEntry] = {
    
    val client = clientList.get(clientId)
    if (client.isEmpty) {//如果客户端没有注册，则报错，拒绝添加Block
      return None
    }
    
    //TODO: [多线程访问控制]申请存储空间时候可能已经有其他客户端开始写入操作
    //通过查看client的blocks(已经写成功)，和pendingEntries(正在写)来确定
    
    spaceManager.checkSpace(size.toInt) match {
      case Some(entryId) => //如果本地有足够的存储空间
        val entry = new SBlockEntry(userDefinedId, entryId, size, true)
        pendingEntries.put(entryId, entry)
        
        Some(entry)
        
      case None => //本地空间不足，需要进行节点迁移，或者远程分配空间，或者返回错误TODO
        val remoteAddress = ""
        val remoteEntry = new SBlockEntry(userDefinedId, 0, size, false)
        //Some(remoteEntry)
        None
    }

  }
  
  /**
   * 写Block结果，在客户端写共享存储成功或者失败后，发消息给worker
   * 新增：
   * 向bsMaster更新Block信息
   */
  def writeBlockResult(clientId: BlockServerClientId, entryId: Int, success: Boolean) = {
    
    pendingEntries.remove(entryId) match {
      case Some(entry) =>
        if (success) {
          assert(entry.entryId == entryId)
          val blockId = blockIndexer.addBlock(entryId, entry)
          //每个Client更新自己的持有Block信息
          clientList.get(clientId).map { client =>
            client.addBlock(blockId, entry)
          }

          if (blockLocation.containsKey(blockId)) {
            blockLocation.get(blockId).add(clientId)
          } else {
            val location = new mutable.HashSet[BlockServerClientId]
            location.add(clientId)
            blockLocation.put(blockId, location)
          }

          //Master这里发送AddBlock异步消息，userDefinedId作为唯一id
          if (worker != null)
            worker.sendMasterBSMessage(ReqbsMasterAddBlock(worker.workerId, entry))
          
          logInfo(s"Block $blockId clientId: $clientId, entryid: $entryId, Write block result successfully")
          Some(blockId)
        } else {//客户端写结果失败
          spaceManager.releaseSpace(entryId, entry.size.toInt)
          logWarning(s"ClientId: $clientId, entry: $entryId, Write block result failed")
          None
        }
        
      case None => //查询不到pending的entry，已经过期被清除，返回失败
        logWarning(s"Pending entry $entryId has been removed, write block failed")
        None
    }
    
  }
  
  /**
   * 得到一个Block信息，返回一个BlockEntry
   * TODO: 在读取的时候，如果另一个应用程序删除怎么办？
   * TODO: 首先先根据血统信息，来判断Block是否存在
   * 
   * 记录BlockLocation信息
   */
  def getBlock(clientId: BlockServerClientId, blockId: SBlockId)= {
    
    val block = blockIndexer.getBlock(blockId)
    if (block.isDefined) {
      //TODO: v2是否去掉blockLocation，因为数据与应用程序去耦合
      if (blockLocation.containsKey(blockId)) {
        blockLocation.get(blockId).add(clientId)
      } else {
        val location = new mutable.HashSet[BlockServerClientId]
        location.add(clientId)
        blockLocation.put(blockId, location)
      }
      block.get.markReadBlock(clientId.appName, isLocal = true)
      //TODO: Master这里是否增加引用计数
      //worker.sendMasterBSMessage(null)
    } else {
      //TODO: 本地不存在Block，是否到远程去查询，这里的是一个同步操作
      //与bsMaster通信，速度会比较慢，所以需要有Worker缓存来抗。除非worker上不存在，这种情况在第一次都会存在。
      //worker.sendMasterBSMessage(message)
    }
    block

  }
  
  /**
   * 删除Block，分为两种情况: 
   * 1) 本地进行请求删除Block
   * 2) 远程进程请求删除Block，或者本地节点根据一定策略来进行删除
   * 
   * ****删除Block是某个App删除，不一定进行物理删除，因为还可能有其他程序正在使用
   * 
   * v1:
   * v2：数据的生命周期与计算脱离，所以本地删除Block并不会传递到bsWorker中
   * 所以删除的情况只有bsWorker因为数据替换和迁移等原因，自己进行删除数据
   * TODO：removeBlock需要重构，去除clientId参数
   */
  def removeBlock(clientId: BlockServerClientId, blockId: SBlockId) = {
    //logDebug(s"Remove block($blockId) from shared memory.")
    val locations = blockLocation.get(blockId)

    if (locations != null) {
      locations -= clientId
      //如果不存在client使用此Block
      //logDebug(s"Remove block($blockId) from shared memory. location: ${locations.size}")
      if (locations.size == 0) {
        
        //从BlockIndexer中删除
        blockIndexer.removeBlock(blockId).map { entry =>
          
          //通知SpaceManager删除，释放空间
          spaceManager.releaseSpace(entry.entryId, entry.size.toInt)
          logInfo(s"Remove block($blockId) success from shared memory.")

          //通知bsMaster发送删除Block信息
          if (worker != null)
            worker.sendMasterBSMessage(ReqbsMasterRemoveBlock(worker.workerId, blockId))
        }
      }
      
      //在clientInfo中去除Block的相关信息在JVM check时候有用
      clientList.get(clientId).map { client => 
        client.removeBlock(blockId)        
      }
      
    }
  }
  
  /**
   * Executor关闭的时候调用
   * TODO：清除所有属于当前Client的Block吗？还是先保留一段时间，过一段时间再做删除操作。
   * 当前做法：
   * 先加入到toRemove队列中，然后由每隔一段时间进行扫描，过一定时间则进行删除
   * 当前时间是否需要先把SpaceManager中的空间释放？不需要，因为物理空间没变。
   * 
   * v1:
   * v2: Executor完毕时候，并不删除缓存数据，数据生命周期与Executor脱离
   */
  def unregClient(clientId: BlockServerClientId) {
    clientList.remove(clientId) match {
      case Some(clientInfo) =>
        logInfo("Trying to remove BlockServerClientInfo " + clientId + " from BlockManagerWorker.")
        //v1&v2: 见上面解释，Executor结束时候并不删除所属的Block
        //removeClientBlock(clientId, clientInfo)
        logDebug("After remove BlockServerClientInfo " + clientId + " from BlockManagerWorker.")
        spaceManager.totalExecutorMemory -= clientInfo.maxJvmMemSize
        //v1: Executor只分配计算内存，
        //spaceManager.totalMemory -= clientInfo.maxMemSize

      case None =>
        logWarning(s"Try to unreg client $clientId, which does not exist.")
    }
  }
  
  /**
   * 删除Client所属的block
   * TODO：需要判断Block是否被其他App正在使用，否则不能进行物理删除
   * 更新：目前在removeBlock中进行实现
   * 
   */
  private def removeClientBlock(clientId: BlockServerClientId, info: BlockServerClientInfo) {
    //logDebug("RemoveClientBlock " + clientId + " from BlockManagerWorker.blocks number: " + info.blocks.size())
    val iterator = info.blocks.keySet().iterator()
    while (iterator.hasNext()) {
      val blockId = iterator.next()
      removeBlock(clientId, blockId)
    }
  }
  
  def updateBlockStatus(clientId: BlockServerClientId, blockId: SBlockId) {
    
  }
  
  /**
   * dan增加读Block的计数
   */
  def markReadBlock(sblockId: SBlockId, appName: String) {
    blockIndexer.getBlock(sblockId).map { entry =>
      entry.markReadBlock(appName)
    }
  }
  

  /**
   * 检查最近没有心跳的client端，然后关闭它
   * 
   * v1&v2：不需要过期检测了，数据与Executor不再耦合
   */
  def expirtDeadClient() {
    logTrace("Checking for hosts with no recent heart beats in client")
  }
  
  /**
   * 检查每个Executor的JVM内存使用状况
   */
  def checkExecutorMemory() {
    //logInfo("****checkExecutorMemory")
    if (clientList.size > 0)
      executorWatcher.check(clientList)
  }

  /**
   * 得到共享存储内存的最大容量
   * @param conf SparkConf
   * @param workerMemory worker节点的内存资源容量
   * @return 共享存储内存的上限
   */
  private def getNodeMaxSMemory(conf: SparkConf, workerMemory: Long): Long = {
    val smemoryFraction = conf.getDouble("spark.smspark.cmemoryFraction", 0.5)
    val safetyFraction = conf.getDouble("spark.storage.safetyFraction", 0.9) 
    val smemory = (workerMemory * smemoryFraction * safetyFraction * 1024 * 1024).toLong
    smemory
  }
}


/**
 *
 */
package org.apache.spark.smstorage.worker

import java.util.{HashMap => JHashMap}
import org.apache.spark.network.{BlockTransferService, BlockDataManager}
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.netty.NettyBlockTransferService
import org.apache.spark.network.nio.NioBlockTransferService
import org.apache.spark.serializer.Serializer
import org.apache.spark.smstorage.migration._
import org.apache.spark.storage.{TestBlockId, BlockNotFoundException, StorageLevel, BlockId}
import scala.collection.mutable.ArrayBuffer
import scala.collection.{mutable, immutable}
import scala.collection.JavaConversions._
import scala.concurrent.duration._
import org.apache.spark.{SparkException, Logging, SparkConf, SparkEnv}
import org.apache.spark.smstorage.BlockServerMessages._
import org.apache.spark.smstorage.BlockServerClientId
import org.apache.spark.smstorage._
import akka.actor.{Actor, ActorRef, Cancellable}
import org.apache.spark.util.{ActorLogReceive, AkkaUtils, Utils, TimeStampedHashMap}
import org.apache.spark.smstorage.sharedmemory.SMemoryManager
import org.apache.spark.deploy.worker.Worker

import scala.util.{Try, Failure, Success}

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
 * spark.smspark.cmemoryFraction 共享内存空间所占的比例
 * spark.smspark.safetyFraction 共享内存空间使用安全比例
 *
 */
private[spark]
class BlockServerWorkerActor(conf: SparkConf, worker: Worker)
  extends Actor with ActorLogReceive with BlockDataManager with Logging {

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
  val initSpaceMemory: Long = if (worker == null) {// for unit test
    conf.get("spark.smspark.initcachedspace").toLong
  } else {
    getNodeMaxSMemory(conf, worker.memory)
  }
  val spaceManager: SpaceManager = new SpaceManager(initSpaceMemory, smManager)

  //val spaceManager: SpaceManager = new SpaceManager(getNodeMaxSMemory(conf, worker.memory), smManager)

  /**
   * is data migration enable
   * if not enable, data will be abondoned if be evicted
   * if enable, data will be first tried to migrate to remote Cached Space if be evicted
   */
  val isMigrationEnabled = conf.getBoolean("spark.smspark.evict.migration.enable", false)

  /**
   * is dynamic allocation enable
   * if not enable, totalCachedSpaceMemory will always be init size
   * if enable, totalCachedSpaceMemory will be dynamically ajusted by the Temporal Space Size
   */
  val isDynamicAllocationEnabled = conf.getBoolean("spark.smspark.da.enable", false)

  val strategy: EvictDataChooseStrategy =
    if (conf.get("spark.smspark.evict.strategy", "LRU").toUpperCase() == "LW") {
      new LinearWeightingStrategy(conf, GlobalStatistic(appTotalCount, totalVisitCount))
    }
    else {
      new LRUStrategy()
    }

  /**
   * 反序列化类，为了反序列化共享存储数据
   */
  val serializer = Helper.instantiateClassFromConf[Serializer](conf,
    "spark.serializer", "org.apache.spark.serializer.JavaSerializer")
  logDebug(s"Using serializer: ${serializer.getClass}")

  /**
   * 索引 Block 的组件
   */
  val blockIndexer: BlockIndexer = new BlockIndexer(strategy)

  /**
   * 监控 Executor 内存运行状况的组件
   */
  val executorWatcher: ExecutorWatcher = new ExecutorWatcher(this, spaceManager, blockIndexer)

  /**
   * 数据传输组件
   */
  var blockTransferService:BlockTransferService = null

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
  
  val checkExecWatchInterval = conf.getLong("spark.smspark.da.executorCheckIntervalMs", 2000)
  
  //////////////////////////////////////////////////////////////////////////////////
  // 统计本节点的一些使用信息
  //////////////////////////////////////////////////////////////////////////////////  
  /**
   * 统计历史访问过数据块的应用Executor个数
   */
  var appTotalCount: Int = 0

  var totalVisitCount: Long = 0l
  
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

    //初始化什么
    blockTransferService =
      conf.get("spark.smspark.blockTransferService", "netty").toLowerCase match {
        case "netty" =>
          new NettyBlockTransferService(conf, worker.securityMgr, 2)
        case "nio" =>
          new NioBlockTransferService(conf, worker.securityMgr)
      }

    blockTransferService.init(this)

    SMSparkContext.blockTransferService = blockTransferService
    SMSparkContext.bsWorker = this

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
   * 客户端bsClient向bsWorker注册
   * 新增：
   * 1）当第一个Executor连接之后开启ExecutorWatch任务
   * 2）向bsMaster更新存储内存的使用信息
   */
  def registerClient(id: BlockServerClientId, maxJvmMemSize: Long, maxMemSize: Long, jvmId: Int, clientActor: ActorRef) = {
    
    if (isDynamicAllocationEnabled && !isFirstExecutorConnected) {//当第一个Executor连接之后开启ExecutorWatch任务
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
      //取消了向bsMaster发送内存使用变化消息，因为这里内存使用没有发生变化
      //if (worker != null)
        //worker.sendMasterBSMessage(ReqbsMasterUpdateSMemory(worker.workerId, spaceManager.totalMemory, spaceManager.usedMemory))
      
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

    //是否本地客户端写入
    val isMigrateWrite = clientId == null

//    val client = clientList.get(clientId)
//    if (client.isEmpty) {//如果客户端没有注册，则报错，拒绝添加Block
//      return None
//    }

    logDebug(s"Begin to request space for $userDefinedId, size: $size")
    //TODO: [多线程访问控制]申请存储空间时候可能已经有其他客户端开始写入操作
    //通过查看client的blocks(已经写成功)，和pendingEntries(正在写)来确定
    var spaceAfterEvict = 0L
    spaceManager.checkSpace(size) match {
      case None => //如果本地有足够的存储空间

      case Some(needSpace) => //本地空间不足，需要进行节点迁移，或者远程分配空间，或者返回错误TODO
        //不进行数据迁移的情况：1)远程数据迁移写入数据 2) 写入size>totalMemory，
        if (isMigrateWrite || needSpace < 0) {
          spaceAfterEvict = size
        } else {
          //进行数据迁移
          logDebug(s"Not enough space, will migrate or evict some block.")
          spaceAfterEvict = doDataMigrateOrEvict(userDefinedId, needSpace)
        }
    }

    //Double check，确保空间的充足
    //assert(spaceManager.checkSpace(size).isEmpty)

    if (spaceAfterEvict <= 0) { //有充足空间，或通过迁移替换得到充足空间
      logDebug(s"Got enough space.")
      val entryId = spaceManager.allocateSpace(size)
      val entry = new SBlockEntry(userDefinedId, entryId, size, true)
      pendingEntries.put(entryId, entry)
      Some(entry)
    } else {
      logDebug(s"Not got enough space after evict or totalMemory. needSpace: " + spaceAfterEvict)
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
//          clientList.get(clientId).map { client =>
//            client.addBlock(blockId, entry)
//          }
//
//          if (blockLocation.containsKey(blockId)) {
//            blockLocation.get(blockId).add(clientId)
//          } else {
//            val location = new mutable.HashSet[BlockServerClientId]
//            location.add(clientId)
//            blockLocation.put(blockId, location)
//          }

          //Master这里发送AddBlock异步消息，userDefinedId作为唯一id
          //都是应该上传数据的哪些信息？globalId，size
          if (worker != null)
            worker.sendMasterBSMessage(ReqbsMasterAddBlock(worker.workerId, entry))
          
          logInfo(s"Block $blockId clientId: $clientId, entryid: $entryId, Write block result successfully")
          Some(blockId)
        } else {//客户端写结果失败
          spaceManager.releaseSpace(entryId, entry.size)
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
      //block还是和executor有从属关系吗？
      clientList.get(clientId).map { client => 
        client.removeBlock(blockId)        
      }
      
    }
  }

  /**
   * Executor关闭的时候调用
   * 当前做法v1:
   * 先加入到toRemove队列中，然后由每隔一段时间进行扫描，过一定时间则进行删除
   * 当前时间是否需要先把SpaceManager中的空间释放？不需要，因为物理空间没变。
   *
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


  /**
   * 向Coordinator报告Block状态，包含被删除
   * @param clientId
   * @param blockId
   */
  def updateBlockStatus(clientId: BlockServerClientId, blockId: SBlockId) {
    //worker.sendMasterBSMessage()
  }
  
  /**
   * dan增加读Block的计数
   * TODO: 客户端缓存的读取是否算数
   */
  def markReadBlock(sblockId: SBlockId, appName: String) {
    blockIndexer.getBlock(sblockId).map { entry =>
      entry.markReadBlock(appName)
    }
  }

  /**
   * 执行数据迁移或替换
   * TODO:数据迁移或者替换，最终仍然可能不能产生足够的空间吗？可能，如果都是同一个RDD下的block
   * 所以应该返回true OR false
   * 1.chooseEvictBlock
   * 2.chooseDestination
   * 3.doMigrate
   * 4.doEvict
   */
  private def doDataMigrateOrEvict(userDefinedId: String, need: Long) = {

    var needSpace = need

    val evictList = blockIndexer.chooseEvictBlock(userDefinedId)

    evictList.forall { case (id, block) =>

      if (isMigrationEnabled) {

        chooseDestination(block) match {
          case Some(dest) =>
            logDebug(s"Trying to migrate $block to $dest.")
            doMigrate(block, dest) match {
              case Success(_) =>
                logDebug(s"Migrated $block to $dest successfully.")
              case Failure(ex) =>
                logDebug(s"Failed to migrate $block to $dest. reason: $ex, Try to evict " + block + " from Shared space.")
            }

          case None =>
            logDebug("Try to evict " + block + " from Shared space.")
        }

      }

      doEvict(id)
      needSpace = needSpace - block.size

      needSpace > 0
    }

    needSpace
  }

  /**
   * TODO: 向bsMaster发送请求，选择合适的目标迁移节点
   * @param block
   * @return
   */
  private def chooseDestination(block: SBlockEntry) = {

    Option(MigrateDestination("", "127.0.0.1", 10075))
  }

  /**
   * TODO: implement data migrate
   *
   * upload nio 使用的是SBlockId封装为BlockMessage
   *
   * @param block
   * @param dest
   * @return
   */
  private def doMigrate(entry: SBlockEntry, dest: MigrateDestination) = {

    //和Spark内存管理中的数据传输方法一致
    Try {

      //通过一个String类型的BlockId来传递数据
      //需要传递哪些信息？entry不需要，blockId需要，当传输之后，也可以找到
      //userDefinedId
      val migrateBlockId = TestBlockId(entry.userDefinedId)

      //根据入口信息，得到数据buffer
      val data = Helper.getBlock(entry)

      //TODO 上传数据，是否需要同步上传？还是异步也可以
      //appId在init时候设置，使用？
      //ExecutorId使用了“migrateServer”
      //使用blockId.toString
      //传输到Server端，然后解析这些参数，存储Block
      //putBlockData是否需要异步进行？
      /**
       * 传递的参数：
       * appId 程序id，未使用到
       * execId 暂时未使用到
       * blockId 使用blockId.toString变为string，在Server端调用BlockId(blockId)还原为blockId
       * data ManagedBuffer，包装了数据的ByteBuffer
       * storageLevel 存储级别，默认为OFF_HEAP
       */
      blockTransferService.uploadBlockSync(dest.host, dest.port, "migrateServer",
        migrateBlockId, data, StorageLevel.OFF_HEAP)

      //报告Coordinator本节点数据增删
      worker.sendMasterBSMessage()
      //

    }

  }

  /**
   * 删除Block
   * @param blockId
   * @return
   */
  private def doEvict(blockId: SBlockId) = {
    //从BlockIndexer中删除
    blockIndexer.removeBlock(blockId) match {

      case Some(entry) =>
        logDebug(s"Trying to evict $blockId.")
        //通知SpaceManager删除，释放空间
        spaceManager.releaseSpace(entry.entryId, entry.size.toInt)
        logInfo(s"Evicted block($blockId) success from shared memory.")

        //通知bsMaster发送删除Block信息
        if (worker != null)
          worker.sendMasterBSMessage(ReqbsMasterRemoveBlock(worker.workerId, blockId))

      case None =>
        logWarning("Try to evict " + blockId + " which not exists in shared space.")

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
    val smemoryFraction = conf.getDouble("spark.smspark.smemoryFraction", 0.5)
    val safetyFraction = conf.getDouble("spark.smspark.safetyFraction", 0.9)
    val smemory = (workerMemory * smemoryFraction * safetyFraction * 1024 * 1024).toLong
    smemory
  }

  /**
   * 得到本地的Block
   * 在远程读取Block数据时候使用
   */
  override def getBlockData(blockId: BlockId): ManagedBuffer = {

    if (!blockId.isInstanceOf[TestBlockId]) {
      throw new SparkException("Get sblock from remote which type is not SBlockId: " + blockId)
    }

    val sid: SBlockId = new SBlockId(blockId.asInstanceOf[TestBlockId].id)
    blockIndexer.getBlock(sid) match {
      case Some(entry) =>
        Helper.getBlock(entry)
      case None =>
        throw new BlockNotFoundException(sid.userDefinedId)
    }
  }

  /**
   * Put the block locally, using the given storage level.
   * 这是一个同步方法，其时间=请求空间+数据写入
   * [smspark]: 在远程数据迁移时调用
   * 1.
   */
  override def putBlockData(blockId: BlockId, data: ManagedBuffer, level: StorageLevel) {

    if (!blockId.isInstanceOf[TestBlockId]) {
      throw new SparkException("Get sblock from remote which type is not SBlockId: " + blockId)
    }

    //从userDefinedId转换为SBlockId
    val globalId = blockId.asInstanceOf[TestBlockId].id
    //val sid: SBlockId = SBlockId(blockId.asInstanceOf[TestBlockId].id)

    //请求空间，这里不需要再进行替换操作
    val timeout = AkkaUtils.lookupTimeout(conf)
    val entryOpt: Option[SBlockEntry] =
      AkkaUtils.askWithReply[Option[SBlockEntry]](
        RequestNewBlock(null, globalId, data.size()), self, timeout)

    entryOpt match {
      case Some(entry) =>
        //write
      Helper.writeBlock(entry, data) match {
          case Success(_) =>
            //write block result
          AkkaUtils.askWithReply[Option[SBlockId]](
            WriteBlockResult(null, entry.entryId, true), self, timeout) match {
            case Some(sid) =>
              assert(sid.userDefinedId == globalId)
            case None =>
              logWarning(s"Write transfer block error!")
          }

          case Failure(ex) =>
            logWarning("Write block failure, will abondon migrated block")
      }
      case None => //空间不足
        logWarning("Not enough space, will abondon migrated block")
    }

  }
}


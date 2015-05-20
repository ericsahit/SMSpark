/**
 *
 */
package org.apache.spark.smstorage.worker

import java.util.{HashMap => JHashMap}
import scala.collection.{mutable, immutable}
import scala.concurrent.duration._
import org.apache.spark.Logging
import akka.actor.Cancellable
import org.apache.spark.SparkConf
import org.apache.spark.smstorage.BlockServerMessages._
import org.apache.spark.smstorage.BlockServerClientId
import org.apache.spark.smstorage._
import akka.actor.{Actor, ActorRef, Cancellable}
import akka.pattern.ask
import org.apache.spark.util.{ActorLogReceive, AkkaUtils, Utils, TimeStampedHashMap}
import org.apache.spark.smstorage.sharedmemory.SMemoryManager

/**
 * @author hwang
 * worker进程中的BlockServerWorker，负责与所有的本机的Executor中BlockServerWorkerRef进行通信
 * 接收Client进行的请求，对于Akka Actor，会把客户端的请求排队
 * TODO：怎么样保存客户端的信息，以及Block的信息
 * TODO：更新Block，都会更新什么状态（可能与其他节点通信）
 * TODO：需要Client的心跳机制吗？
 * TODO: 每一个Block保持一个计数器，如果没有任何程序使用，表明可以删除。如果有计数器不为0，可以被替换或者
 * TODO: Worker节点向Client发送命令
 * TODO：SBlock的id匹配机制，根据RDD血统信息来进行匹配
 * TODO：各个组件的清理工作
 */
private[spark]
class BlockServerWorkerActor(conf: SparkConf)  
  extends Actor with ActorLogReceive with Logging {
  
  val smManager: SMemoryManager = new SMemoryManager()
  //初始内存，可以设定一个配置的值
  val spaceManager: SpaceManager = new SpaceManager(0, smManager)
  val blockIndexer: BlockIndexer = new BlockIndexer()  
  
  
  private val clientList = new mutable.HashMap[BlockServerClientId, BlockServerClientInfo]
  
  //保存Block列表
  private val blocks = new TimeStampedHashMap[SBlockId, SBlockEntry]
  
  //保存Block位置，可能有多个Client。保存Block的位置可以表示有多少人在使用它
  private val blockLocation = new mutable.HashMap[SBlockId, mutable.HashSet[BlockServerClientId]]
  
  //保存被锁定的空间, entryId->SBlockEntry
  //TODO：需要过期清理
  private val pendingEntries = new TimeStampedHashMap[Int, SBlockEntry]
  
  private val pendingClients = new TimeStampedHashMap[String, BlockServerClientId]
  
  var timeoutCheckingTask: Cancellable = null
  
  private val akkaTimeout = AkkaUtils.askTimeout(conf)
  
  val checkTimeoutInterval = conf.getLong("spark.storage.blockManagerTimeoutIntervalMs", 60000)
  
  override def preStart() {
    logInfo("Starting Spark BlockServerWorker")
    import context.dispatcher
    //定期运行监测client是否失去链接
    timeoutCheckingTask = context.system.scheduler.schedule(
        0.seconds, 
        checkTimeoutInterval.seconds,
        self,
        ExpireDeadClient)
    
    super.preStart()
  }
  
  /**
   * TODO：清理工作
   */
  override def postStop() {
    logInfo("Clean shared memory space when worker closed")
    def clearEntry(entry: SBlockEntry) = spaceManager.releaseSpace(entry.entryId, entry.size.toInt)
    pendingEntries.values.foreach(clearEntry)
    blockIndexer.clear(clearEntry)
  }
  
  
  
  override def receiveWithLogging = {
    
    case RegisterBlockServerClient(clientId, maxMemSize, jvmId, clientActor) =>
      registerClient(clientId, maxMemSize, jvmId, clientActor)
      sender ! true
      
    case RequestNewBlock(clientId, name, size) =>
      sender ! reqNewBlock(clientId, name, size)
      
    case WriteBlockResult(clientId, entryId, success) =>
      sender ! writeBlockResult(clientId, entryId, success)
    
    case GetBlock(blockId) =>
      sender ! getBlock(blockId)
    
    case ExpireDeadClient =>
      expirtDeadClient()
      
    case CheckExecutorMemory =>
      checkExecutorMemory()
      
    case RemoveBlock(blockId) =>
      removeBlock(blockId)
      sender ! true
      
     
//    case UpdateBlockStatus(clientId, blockId) =>
//      updateBlockStatus(clientId, blockId)
      
    case BlockContains(blockId, local) => //TODO：是否本地？
      sender ! blockIndexer.contains(blockId)


    case other =>
      logWarning("unknown blockServerClient message: " + other)
  }
  
  /**
   * 注册客户端
   */
  def registerClient(id: BlockServerClientId, maxMemSize: Long, jvmId: Int, clientActor: ActorRef) = {
    if (!clientList.contains(id)) {
      
      clientList(id) = new BlockServerClientInfo(id, System.currentTimeMillis(), maxMemSize, jvmId, clientActor)
      
      spaceManager.totalMemory += maxMemSize
      
      
      logInfo("Registering block server client %s with %s RAM, JVMID %d, %s".format(
        id.hostPort, Utils.bytesToString(maxMemSize), jvmId, id))
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
   * 
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
          logInfo(s"Block $blockId clientId: $clientId, entryid: $entryId, Write block result successfully")
          Some(blockId)
        } else {//客户端写结果失败
          spaceManager.releaseSpace(entryId, entry.size.toInt)
          logWarning(s"ClientId: $clientId, entry: $entryId, Write block result failed")
          None
        }
        
      case None => //查询不到pending的entry，已经过期被清除，返回失败
        logWarning("Pending entry has been removed, write block failed")
        None
    }
    
  }
  
  /**
   * 得到一个Block信息，返回一个BlockEntry
   * TODO: 在读取的时候，如果另一个应用程序删除怎么办？
   * TODO: 首先先根据血统信息，来判断Block是否存在
   * 
   */
  def getBlock(blockId: SBlockId)= {
    blockIndexer.getBlock(blockId)
  }
  
  /**
   * 删除Block，分为两种情况，本地进行请求删除Block，远程进程请求删除Block，或者本地节点根据一定策略来进行删除
   * TODO: 遍历blockLocation，查找client，通知进程删除？
   * 
   */
  def removeBlock(blockId: SBlockId) = {
    
    blockIndexer.removeBlock(blockId).map { entry =>
      blockLocation.remove(blockId).map { locations =>
        locations.foreach { clientId =>
          clientList.get(clientId).map {client =>
            client.removeBlock(blockId)
            //通知本地的其他进程进程，删除Block
            //TODO：通知协调者节点，或者其他的worker进程？
            //client.clientActor.ask(RemoveBlock(blockId))(akkaTimeout)
          }
        }
      }
      spaceManager.releaseSpace(entry.entryId, entry.size.toInt)
    }
  }
  
  def updateBlockStatus(clientId: BlockServerClientId, blockId: SBlockId) {
    
  }
  

  /**
   * 检查最近没有心跳的client端，然后关闭它
   * 这里需要有个
   * TODO
   */
  def expirtDeadClient() {
    logTrace("Checking for hosts with no recent heart beats in client")
  }
  
  /**
   * 检查每个Executor的JVM内存使用状况
   */
  def checkExecutorMemory() {
    
  }
}


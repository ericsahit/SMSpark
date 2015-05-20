/**
 *
 */
package org.apache.spark.smstorage.client

import org.apache.spark.Logging
import akka.actor.ActorRef
import org.apache.spark.SparkConf
import org.apache.spark.util.AkkaUtils
import org.apache.spark.SparkException
import org.apache.spark.smstorage.BlockServerClientId
import org.apache.spark.smstorage.SBlockId
import org.apache.spark.smstorage.BlockServerMessages._
import org.apache.spark.smstorage.SBlockEntry
import java.util.concurrent.ConcurrentHashMap

/**
 * @author hwang
 * 与BlockServerWorker进行通信发送请求
 * TODO
 */
class BlockServerClient(val clientId: BlockServerClientId, var blockServerWorkerActor: ActorRef, conf: SparkConf)
  extends Logging {
  
  private val AKKA_RETRY_ATTEMPTS = AkkaUtils.numRetries(conf)
  private val AKKA_RETRY_INTERVAL_MS = AkkaUtils.retryWaitMs(conf)
  val timeout = AkkaUtils.askTimeout(conf)

  /**
   * 从共享存储管理中去除一个Executor，包含他的内存和所包含的Block
   * TODO: ****传递什么样格式的消息
   */
  def removeExecutor(execId: String) {
    tell(RemoveExecutor(execId))
    logInfo("Removed " + execId + " successfully in removeExecutor")
  }
  
  /**
   * 向BlockServer注册BlockServerClient
   * 
   * maxMemSize
   * 
   */
  def registerClient(maxMemSize: Long, jvmId: Int, clientActor: ActorRef) {
    logInfo("Trying to register BlockServerClient")
    tell(RegisterBlockServerClient(clientId, maxMemSize, jvmId, clientActor))
    logInfo("Registered BlockServerClient")
  }
  
  /**
   * 更新Block状态，在什么场合下面使用
   * TODO：更新size信息，移除Block等
   */
  def updateBlockInfo(): Boolean = {
    false
  }
  
  def getBlockSize(blockId: SBlockId): Long = {
    //先查找block的本地列表. 
    //TODO：需要访问worker节点增加数据读取计数吗？在共存存储空间那增加计数貌似更好
    0L
  }
  
  /**
   * 查看Block是否存在
   */
  def contains(blockId: SBlockId): Boolean = {
    askBlockServerWithReply[Option[SBlockEntry]](GetBlock(blockId)).isDefined
  }
  
  /**
   * 从共享存储得到一个Block，返回BlockEntry信息，其中包含Block
   * TODO：askBlockServerWithReply超时了怎么办？
   */
  def getBlock(blockId: SBlockId): Option[SBlockEntry] = {
      askBlockServerWithReply[Option[SBlockEntry]](GetBlock(blockId))
  }
  
  /**
   * 增加一个Block，先得到共享存储的入口信息，然后开始进行写入
   * 参数：
   */
  def reqNewBlock(userDefinedId: String, size: Long): Option[SBlockEntry] = {
    askBlockServerWithReply[Option[SBlockEntry]](RequestNewBlock(clientId, userDefinedId, size))
  }
  
  /**
   * 写共享存储空间完成后
   */
  def writeBlockResult(entryId: Int, success: Boolean): Option[SBlockId] = {
    askBlockServerWithReply[Option[SBlockId]](WriteBlockResult(clientId, entryId, success))
  }
  

  
  /**
   * 移除某一个Block
   */
  def removeBlock(blockId: SBlockId) = {
    askBlockServerWithReply[Boolean](RemoveBlock(blockId))
  }
  
  /**
   * 停止，是否需要删除所有Block
   * 通知Worker节点，删除本client的所有block
   */
  def stop() {
    
  }
  
    

  
  /**
   * 向BlockServer发送一个消息，并且希望获得一个true的返回结果，否则抛出异常
   * TODO：****异常怎么处理？
   */
  private def tell(message: Any) {
    if (!askBlockServerWithReply[Boolean](message)) {
      throw new SparkException("BlockServer return false, expect true")
    }
  }
  
  /**
   * 包装了同步的Akka调用，阻塞直到返回结果
   */
  private def askBlockServerWithReply[T](message: Any): T = {
    AkkaUtils.askWithReply(message, blockServerWorkerActor, AKKA_RETRY_ATTEMPTS, AKKA_RETRY_INTERVAL_MS, timeout)
  }
  
  
}
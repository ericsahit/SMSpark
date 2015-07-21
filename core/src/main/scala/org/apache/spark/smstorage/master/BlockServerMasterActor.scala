/**
 *
 */
package org.apache.spark.smstorage.master

import java.util.{HashMap => JHashMap}

import scala.collection.mutable
import scala.collection.JavaConversions._
import scala.concurrent.Future
import scala.concurrent.duration._

import akka.actor.{Actor, ActorRef, Cancellable}
import akka.pattern.ask

import org.apache.spark.{Logging, SparkConf, SparkException}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.scheduler._
import org.apache.spark.storage.BlockManagerMessages._
import org.apache.spark.util.{ActorLogReceive, AkkaUtils, Utils}

/**
 * @author hwang
 * Master（Coordinator）节点上的Actor
 * 
 * 主要用来管理多个Worker节点的资源使用情况，和Block的Location情况
 * 功能：
 * 1）管理元数据：包含RDD所含有的Block状态，分配节点状态
 * 2）选举一个节点，方便Worker节点做数据的迁移
 * 
 * 1) bsWorker注册自己，需要上报自己的节点信息，内存上限信息，
 * 同时bsWorker定期定期上报自己的内存使用信息，Block更新信息，
 * 2) bsWorker上报自己的RDD Block情况，bsMaster保存Block的位置信息，方便的
 * 
 * 
 * TODO：Coordinator需要掌握Worker的Actor吗？
 */
class BlockServerMasterActor(conf: SparkConf)
  extends Actor with ActorLogReceive with Logging {  
  
  /**
   * 保存Worker的情况，workerId和WorkerInfo
   */
  private val bsWorkerInfo = new mutable.HashMap[String, String]()
  
  val slaveTimeout = conf.getLong("spark.storage.blockManagerSlaveTimeoutMs", 120 * 1000)

  val checkTimeoutInterval = conf.getLong("spark.storage.blockManagerTimeoutIntervalMs", 60000)
  
  var timeoutCheckingTask: Cancellable = null
  
  override def preStart() {
    import context.dispatcher
    timeoutCheckingTask = context.system.scheduler.schedule(0.seconds,
      checkTimeoutInterval.milliseconds, self, ExpireDeadHosts)
    super.preStart()
  }
  
    override def receiveWithLogging = {
//    case RegisterBSWorker(bsWorkerId, maxMemSize, bsWorkerActor) =>
//      register(blockManagerId, maxMemSize, slaveActor)

    case ExpireDeadHosts =>
      expireDeadHosts()
      
    case other =>
      logWarning("Got unknown message: " + other)
  }
    
  def expireDeadHosts() {
    
  }
  
  
  
  
  
}
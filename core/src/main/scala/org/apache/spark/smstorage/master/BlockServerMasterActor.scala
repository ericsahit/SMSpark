/**
 *
 */
package org.apache.spark.smstorage.master

import akka.actor.Actor
import org.apache.spark.util.ActorLogReceive
import org.apache.spark.Logging
import scala.collection.mutable

/**
 * @author hwang
 * Master（Coordinator）节点上的Actor
 * 
 * 主要用来管理多个Worker节点的资源使用情况，和Block的Location情况
 * 功能：
 * 1）管理元数据
 * 2）选举一个节点，方便Worker节点做数据的迁移
 * 
 * TODO：Coordinator需要掌握Worker的Actor吗？
 */
class BlockServerMasterActor
  extends Actor with ActorLogReceive with Logging {  
  
  /**
   * 保存Worker的情况，workerId和WorkerInfo
   */
  private val blockWorkerInfo = new mutable.HashMap[String, String]()
  
  override def receiveWithLogging = {
    null
  }
  
  
  
}
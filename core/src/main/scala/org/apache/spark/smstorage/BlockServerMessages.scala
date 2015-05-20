/**
 *
 */
package org.apache.spark.smstorage

import akka.actor.ActorRef

/**
 * @author hwang
 * 消息传递的格式
 *
 */
private[spark] object BlockServerMessages {
  
  //////////////////////////////////////////////////////////////////////////////////
  // Messages from the worker to client.
  //////////////////////////////////////////////////////////////////////////////////
  sealed trait BlockServerWorkerToClient
  
  case object ExpireDeadClient extends BlockServerWorkerToClient
  
  //检查Executor的内存
  case object CheckExecutorMemory extends BlockServerWorkerToClient
  
  //////////////////////////////////////////////////////////////////////////////////
  // Messages from client to the worker.
  //////////////////////////////////////////////////////////////////////////////////
  sealed trait BlockServerClientToWorker
  
  case class RegisterBlockServerClient(
      blockServerClientId: BlockServerClientId,
      maxMemorySize: Long,
      jvmId: Int,
      clientActor: ActorRef) 
      extends BlockServerClientToWorker
  
  case class RemoveBlock(blockId: SBlockId) extends BlockServerClientToWorker
  
  case class RemoveExecutor(execId: String) extends BlockServerClientToWorker
  
  case class GetBlockStatus(blockId: SBlockId) extends BlockServerClientToWorker
  
  case class GetBlockLocation(blockId: SBlockId) extends BlockServerClientToWorker
  
  case class GetBlockSize(blockId: SBlockId) extends BlockServerClientToWorker

  case class BlockContains(blockId: SBlockId, local: Boolean = true) extends BlockServerClientToWorker
  
  //读Block
  case class GetBlock(blockId: SBlockId) extends BlockServerClientToWorker
  
  //写Block
  case class RequestNewBlock(clientId: BlockServerClientId, userDefinedId: String, size: Long) extends BlockServerClientToWorker
  
  case class WriteBlockResult(clientId: BlockServerClientId, entryId: Int, success: Boolean) extends BlockServerClientToWorker
  
  
}
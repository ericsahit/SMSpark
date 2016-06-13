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
  // Messages from the worker to master.
  // bsWorker到bsMaster之间的消息
  //////////////////////////////////////////////////////////////////////////////////  
  sealed trait BlockServerWorkerToMaster
  
  //向bsMaster注册bsWorker
  //case class RegisterBSWorker(bsWorkerId: , maxMemSize: Long, bsWorkerActor: ActorRef) extends BlockServerWorkerToMaster
  
  //更新当前Worker的使用内存
  case class ReqbsMasterUpdateSMemory(workerId: String, memoryTotal: Long, memoryUsed: Long) extends BlockServerWorkerToMaster
  //新增Block
  case class ReqbsMasterAddBlock(workerId: String, blockEntry: SBlockEntry) extends BlockServerWorkerToMaster
  //请求bsMaster删除一个Block
  case class ReqbsMasterRemoveBlock(workerId: String, blockEntry: SBlockId) extends BlockServerWorkerToMaster
  //Driver向bsMaster请求Block的位置，查询是否存在已有的缓存数据
  case class ReqbsMasterGetLocations(blockIds: Array[SBlockId]) extends BlockServerWorkerToMaster

  //bsWorker向bsMaster请求选取目标迁移节点
  case class ReqChooseMigrateDesination(workerId: String, entry: SBlockEntry) extends BlockServerWorkerToMaster

  case class MigrateDestination(workerId: String, host: String, port: Int)
  
  //////////////////////////////////////////////////////////////////////////////////
  // Messages from the worker to client.
  //////////////////////////////////////////////////////////////////////////////////
  sealed trait BlockServerWorkerToClient
  
  case object ExpireDeadClient extends BlockServerWorkerToClient
  
  //检查Executor的内存
  case object CheckExecutorMemory extends BlockServerWorkerToClient
  
  //////////////////////////////////////////////////////////////////////////////////
  // Messages from WorkerDaemon to the bsworker.
  // Worker到bsWorker的消息
  //////////////////////////////////////////////////////////////////////////////////  
  sealed trait WorkerDaemonToBSWorker
  
  case class RegisteredMasterDaemon(masterUrl: String) extends WorkerDaemonToBSWorker
  
  
  
  //////////////////////////////////////////////////////////////////////////////////
  // Messages from client to the worker.
  //////////////////////////////////////////////////////////////////////////////////
  sealed trait BlockServerClientToWorker
  
  case class RegisterBlockServerClient(
      blockServerClientId: BlockServerClientId,
      maxJvmMemSize: Long,
      maxMemorySize: Long,
      jvmId: Int,
      clientActor: ActorRef) 
      extends BlockServerClientToWorker
  
  case class RemoveBlock(clientId: BlockServerClientId, blockId: SBlockId) extends BlockServerClientToWorker
  
  case class RemoveExecutor(execId: String) extends BlockServerClientToWorker
  
  case class UnregisterBlockServerClient(blockServerClientId: BlockServerClientId) extends BlockServerClientToWorker
  
  case class GetBlockStatus(blockId: SBlockId) extends BlockServerClientToWorker
  
  case class GetBlockLocation(blockId: SBlockId) extends BlockServerClientToWorker
  
  case class GetBlockSize(blockId: SBlockId) extends BlockServerClientToWorker

  case class BlockContains(blockId: SBlockId, local: Boolean = true) extends BlockServerClientToWorker
  
  //读Block
  case class GetBlock(clientId: BlockServerClientId, blockId: SBlockId) extends BlockServerClientToWorker
  
  //读Block时候增加计数
  case class ReadSBlock(sblockId: SBlockId, appName: String) extends BlockServerClientToWorker
  
  //写Block
  case class RequestNewBlock(clientId: BlockServerClientId, userDefinedId: String, size: Long) extends BlockServerClientToWorker
  
  case class WriteBlockResult(clientId: BlockServerClientId, entryId: Int, success: Boolean) extends BlockServerClientToWorker
  
  //////////////////////////////////////////////////////////////////////////////////
  // Messages from WorkerDaemon to the bsworker.
  // bsWorker到bsMaster之间的消息
  //////////////////////////////////////////////////////////////////////////////////  
  //sealed trait BSWorkerToBSMaster
  
  
}
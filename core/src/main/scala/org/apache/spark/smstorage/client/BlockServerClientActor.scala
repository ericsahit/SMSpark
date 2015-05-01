/**
 *
 */
package org.apache.spark.smstorage.client

/**
 * @author hwang
 * Executor中负责SharedMemoryStore功能的Actor，是BlockServerWorker的客户端，负责执行指定BlockServerWorker的命令
 * 
 * TODO: 向client发送删除本地的Block信息
 * client.clientActor.ask(RemoveBlock(blockId))(akkaTimeout)
 * 
 */
class BlockServerClientActor {

}
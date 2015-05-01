/**
 *
 */
package org.apache.spark.smstorage.worker

/**
 * @author hwang
 * Worker节点上的BlockServer.负责：
 * 管理本地节点上所有Executor的RDD Block；
 * 与BlockServerClient进行通信，收集各个Executor的数据使用，同一管理，响应Executor的数据请求；
 * 与BlockServerMaster进行通信，上报自己的空间使用情况和数据块使用情况。
 * 与其他BlockServerWorker进行通信，进行数据远程传递和接收。
 */
class BlockServerWorker {
  
}
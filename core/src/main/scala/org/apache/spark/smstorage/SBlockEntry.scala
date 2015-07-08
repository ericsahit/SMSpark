/**
 *
 */
package org.apache.spark.smstorage

/**
 * @author hwang
 * 
 * 存储SharedBlock的相关信息，包含:
 * 
 * userDefinedId: 全局唯一的标识，参见SBlockId中的解释
 * 
 * Block的共享存储入口信息，entryid
 * 是否存在本地
 */
private[spark] class SBlockEntry (
    val userDefinedId: String,
    //val smtype: String,
    val entryId: Int,
    val size: Long,
    val local: Boolean)
  extends Serializable {
  
  /**
   * 数据块构建代价，属于同一个RDD的构建代价都一样
   * 使用构建此Block需要读入blk个数据块，进行trans个变换操作，包含shuf个shuffle操作，赋予这三种不同的权值来量化
   */
  var cons = 1.0
  
  /**
   * 最近访问时间，当被访问时候更新
   * 数据的访问最近访问时间量化：dr=1/now-lastAccess
   */
  @transient var lastReadTime = System.currentTimeMillis()
  
  /**
   * 数据块的访问次数，访问一次+1
   * 数据块的访问频度量化：
   */
  var visitCount: Int = 0
  
  /**
   * 数据块被访问的应用个数，当有新应用访问此数据块时候+1
   */
  var visitAppCount: Int = 0
  
  /**
   * 数据块本地化访问次数，当本地访问一次+1
   */
  var visitLocalCount: Int = 0
  
  /**
   * 数据块非本地化访问次数，非本地访问一次+1
   */
  var visitRemoteCount: Int = 0
  
  
  //表明是否正在读写
  @transient var pending = false
  //最后一次访问时间
  
  //Coordinator使用，当前有几个程序正在使用
  @transient var usingWorkers = new scala.collection.mutable.HashSet[String]
  
  def updateReadTime() = {
    lastReadTime = System.currentTimeMillis()
  }
  
  /**
   * 更新BlockEntry，目前就是更新最后读取时间，以及更新引用的WorkerId
   */
  def update(workerId: String, newBlockEntry: SBlockEntry) {
    updateReadTime()
    usingWorkers += (workerId)
  }
  
  def remove(workerId: String) = {
    usingWorkers -= workerId
  }
  
}
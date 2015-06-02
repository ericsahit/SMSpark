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
  
  //表明是否正在读写
  @transient var pending = false
  //最后一次访问时间
  @transient var lastReadTime = System.currentTimeMillis()
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
/**
 *
 */
package org.apache.spark.smstorage

/**
 * @author hwang
 * 
 * 存储SharedBlock的相关信息，包含:
 * 
 * Block的共享存储入口信息，entryid
 * 是否存在本地
 */
private[spark] class SBlockEntry (
    val name: String,
    //val smtype: String,
    val entryStr: String,
    val size: Long,
    val local: Boolean)
  extends Serializable {
  
  //表明是否正在读写
  @transient var pending = false
  //最后一次访问时间
  @transient var lastReadTime = System.currentTimeMillis()
  
  def updateReadTime() = {
    lastReadTime = System.currentTimeMillis()
  }
  
}
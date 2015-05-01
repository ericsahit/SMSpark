/**
 *
 */
package org.apache.spark.smstorage

import org.apache.spark.storage.BlockId

/**
 * @author hwang
 * 共享Block的id，包含必要的传递信息，以及识别
 * TODO：从SBlockId到BlockId的转换，使用隐式转换？
 * TODO：SBlockId与RDD的对应关系
 */
class SBlockId(
    val localBlockId: String,
    val rddDepId: Long,
    val name: String = "") {
  
  /**
   * shared memory entry string，作为唯一的id
   * 每一个共享的BlockId中，共享存储空间都是唯一的
   * entry是否过会很长？
   */
  //def name: String
  
  /**
   * 本地Block的id
   */
  //def localBlockId: String
  
  /**
   * RDD的id，作为判断Block是否相同的判断条件
   * TODO: 由输入+每次的变换+下一个Stage的partition+下一个Stage的变换+...组成
   */
  //def rddDepId: Long
  
  override def toString = if (name.isEmpty()) rddDepId.toString else name
  override def hashCode = if (name.isEmpty()) rddDepId.hashCode else name.hashCode
  override def equals(other: Any): Boolean = other match {
    case id: SBlockId =>
      if (!name.isEmpty()) {
        name.equals(id.name)
      } else {
        rddDepId == id.rddDepId
      }
  }
}

object SBlockId {
  val RDD = "rdd_([0-9]+)_([0-9]+)".r 
  
  def apply(id: String) = id match {
    case RDD(rddId, splitIndex) =>
      //SBlockId("rdd_" + rddId + "_" + splitIndex)
  }
  
  def apply(localBlockId: BlockId, rddDepId: Long = 0L) = {
    new SBlockId(localBlockId.name, rddDepId)
  }
}
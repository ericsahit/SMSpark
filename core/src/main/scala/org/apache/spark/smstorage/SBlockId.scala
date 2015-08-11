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
 * TODO: 在local block id中增加user defined rdd name
 * 使用user defined rdd name+splitIndex作为userDefinedId，作为全局唯一识别的id
 * 格式：grdd|[userDefinedId]|[splitIndex]
 * 
 * 新增SRDD的Block时，先传入SRDD名称，和SplitIndex，判断SRDD是否存在：
 * 1.如果已经存在，则直接返回Entry信息
 * 2.如果不存在，进行写SBlock流程，先返回共享存储的Entry，客户端开始写Entry，写成功之后，返回BlockId
 * TODO：在BlockManager原有的查找Block的方法中，增加在如果SBLock，则通过LocalMemoryStore做一次远程的查找
 * 
 * localBlockId：本地的blockId
 * globalBlockId：全局的blockId，由用户指定，来识别有同样的血统关系。
 * name：全局的id，存储entryId，block写入成功前为空，否则也是全局唯一（****entryId不一定是全局唯一，多个节点之间可能会冲突）
 */
class SBlockId(
    val userDefinedId: String,
    val localBlockId: String = "",
    var name: String = "") extends Serializable {
  
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
   * ****现在以enrtyId为优先判断条件，然后以userDefinedId为判断条件;是否改成以userDefinedId为主要判断条件
   */
  //def rddDepId: Long
  
  override def toString = s"$userDefinedId,$localBlockId,$name"
  override def hashCode = if (userDefinedId.isEmpty()) name.hashCode else userDefinedId.hashCode
  override def equals(other: Any): Boolean = other match {
    case o: SBlockId =>
      if (!userDefinedId.isEmpty() && !userDefinedId.contains("noverifyuserid")) {
        userDefinedId.equals(o.userDefinedId)
      } else {
        name.equals(o.name) && localBlockId.equals(o.localBlockId)
        //name == o.name
      }

  }
}

object SBlockId {
  val RDD = "rdd_([0-9]+)_([0-9]+)".r
  val SRDD = "srdd|([0-9]+)|([0-9]+)".r 
  
  /**
   * worker端用来匹配userDefinedId，生成SBlock并且进行查找SBlock是否存在
   * 使用userDefinedId来匹配，测试使用RDD
   * TODO：userDefinedId格式的确定
   */
  def apply(userDefinedId: String) = userDefinedId match {
    case RDD(rddId, splitIndex) =>
      new SBlockId("rdd_" + rddId + "_" + splitIndex)
    case SRDD(srddId, splitIndex) =>
      new SBlockId("srdd_" + srddId + "_" + splitIndex)
    case _ =>
      throw new IllegalStateException("Unrecognized userDefinedId: " + userDefinedId)
  }
  
  /**
   * local block和sblock之间的转换
   * 使用user defined rdd name+splitIndex作为userDefinedId，作为全局唯一识别的id
   * 格式：grdd|[userDefinedId]|[splitIndex]
   * 目前格式：[userDefinedId]|[splitIndex]，在CacheManager.get中生成
   * 
   */
  def apply(localBlockId: BlockId, appName: String = "") = {
    val rddBlockId = localBlockId.asRDDId
    if (rddBlockId.isEmpty) {
      throw new IllegalStateException("try to parse sblock which is not RDDBlock type")
    }
    
    val userId = rddBlockId.get.userDefinedId
    if (userId == null || userId.isEmpty()) {
      //new SBlockId(appName + "|" + rddBlockId.get.name, rddBlockId.get.name)
      new SBlockId("|" + rddBlockId.get.name, rddBlockId.get.name)
    } else {
      new SBlockId(userId, rddBlockId.get.name)
    }
  }
}
/**
 *
 */
package org.apache.spark.smstorage

import java.io.Externalizable
import java.util.concurrent.ConcurrentHashMap
import org.apache.spark.util.Utils
import java.io.ObjectOutput
import java.io.ObjectInput

/**
 * @author hwang
 * 代表本地Block Server Worker的唯一id，跟Worker一一对应，使用host，port来区分。
 * 向BlockServer注册时候使用
 * 包含：executorId，host，port
 * 
 * TODO: ****KryoSerializer中含有序列化的类
 * TODO: ****
 */
private[spark] class BlockServerWorkerId (
    private var host_ : String,
    private var port_ : Int)
  extends Externalizable {
  
  private def this() = this( null, 0)
  
  if (null != host_) {
    Utils.checkHost(host_, "Expected hostname")
    assert(port_ > 0)
  }
  
  def host: String = host_
  
  def port: Int = port_
  
  def hostPort: String = {
    Utils.checkHost(host)
    assert(port > 0)
    host + ":" + port
  }
  
  override def writeExternal(out: ObjectOutput): Unit = Utils.tryOrIOException {
    out.writeUTF(host_)
    out.writeInt(port_)
  }
  
  override def readExternal(in: ObjectInput): Unit = Utils.tryOrIOException {
    host_ = in.readUTF()
    port_ = in.readInt()
  }
  
  override def toString() = s"BlockServerWorkerId($host, $port)"
  
  override def hashCode: Int = host.hashCode * 41 + port.hashCode
  
  /**
   * 重载equals函数，判断当两者相等时，BlockServerWorkerId相等
   */
  override def equals(that: Any) = that match {
    case id: BlockServerClientId =>
      host == id.host && port == id.port
    case _ =>
      false
  }
  
}

private[spark] object BlockServerWorkerId {
  
  def apply(execId: String, host: String, port: Int) = {
    get(new BlockServerWorkerId(host, port))
  }
  
  def apply(in: ObjectInput) = {
    val newId = new BlockServerWorkerId()
    newId.readExternal(in)
    get(newId)
  }
  
  val CACHE = new ConcurrentHashMap[BlockServerWorkerId, BlockServerWorkerId]()
  
  def get(id: BlockServerWorkerId) = {
    CACHE.putIfAbsent(id, id)
    CACHE.get(id)
  }
}
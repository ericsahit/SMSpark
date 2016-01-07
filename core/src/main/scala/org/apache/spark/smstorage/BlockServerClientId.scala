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
 * 代表本地MemoryStore的唯一id，每一个Executor，使用executorId，host，port来区分。
 * 向BlockServer注册时候使用
 * 包含：executorId，host，port
 * 
 * TODO: ****KryoSerializer中含有序列化的类
 * TODO: ****
 */
private[spark] class BlockServerClientId (
    private var executorId_ : String,
    private var host_ : String,
    private var port_ : Int,
    private var appName_ : String=null)
  extends Externalizable {
  
  private def this() = this(null, null, 0, null)
  
  if (null != host_) {
    Utils.checkHost(host_, "Expected hostname")
    assert(port_ > 0)
  }
  
  def executorId = executorId_
  
  def host: String = host_
  
  def port: Int = port_
  
  def appName: String = appName_
  
  def hostPort: String = {
    Utils.checkHost(host)
    assert(port > 0)
    host + ":" + port
  }
  
  override def writeExternal(out: ObjectOutput): Unit = Utils.tryOrIOException {
    out.writeUTF(executorId_)
    out.writeUTF(host_)
    out.writeInt(port_)
    out.writeUTF(appName_)
  }
  
  override def readExternal(in: ObjectInput): Unit = Utils.tryOrIOException {
    executorId_ = in.readUTF()
    host_ = in.readUTF()
    port_ = in.readInt()
    appName_ = in.readUTF()
  }
  
  override def toString() = s"BlockServerClientId($executorId, $host, $port)"
  
  override def hashCode: Int = (executorId.hashCode * 41 + host.hashCode) * 41 + port.hashCode
  
  /**
   * 重载equals函数，判断当三者相等时，BlockServerClientId相等
   */
  override def equals(that: Any) = that match {
    case id: BlockServerClientId =>
      executorId == id.executorId && host == id.host && port == id.port
    case _ =>
      false
  }
  
}

private[spark] object BlockServerClientId {
  
  def apply(execId: String, host: String, port: Int, appName: String = "") = {
    get(new BlockServerClientId(execId, host, port, appName))
  }
  
  def apply(in: ObjectInput) = {
    val newId = new BlockServerClientId()
    newId.readExternal(in)
    get(newId)
  }
  
  val CACHE = new ConcurrentHashMap[BlockServerClientId, BlockServerClientId]()
  
  def get(id: BlockServerClientId) = {
    CACHE.putIfAbsent(id, id)
    CACHE.get(id)
  }
}
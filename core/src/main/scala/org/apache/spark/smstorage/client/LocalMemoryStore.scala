/**
 *
 */
package org.apache.spark.smstorage.client

import scala.collection.Iterator
import java.nio.ByteBuffer
import org.apache.spark.Logging
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.storage.BlockId
import org.apache.spark.storage.BlockManager
import org.apache.spark.storage.BlockStore
import org.apache.spark.storage.PutResult
import org.apache.spark.storage.StorageLevel
import org.apache.spark.smstorage.SBlockId
import org.apache.spark.util.Utils
import java.io.IOException
import org.apache.spark.smstorage.client.io.BlockInputStream
import org.apache.spark.smstorage.client.io.LocalBlockInputStream
import com.google.common.io.ByteStreams
import java.io.InputStream
import java.util.concurrent.ConcurrentHashMap
import org.apache.spark.smstorage.SBlockEntry
import java.io.OutputStream
import java.util.HashMap
import org.apache.spark.smstorage.SBlockEntry
import org.apache.spark.smstorage.SBlockEntry
import org.apache.spark.smstorage.client.io.LocalBlockOutputStream

/**
 * @author hwang
 * SharedMemoryStore的客户端，LocalMemoryStore
 * 负责替换原生的MemoryStore
 * 所以对外所需要的功能不能变，所以需要分析清楚堆外所提供的所有功能
 * 
 * 存放的Block都是什么类型，RDD，shuffle，
 * Executor 238：TaskResultBlockId也会存储到BlockManager中
 * env.blockManager.putBytes(blockId, serializedDirectResult, StorageLevel.MEMORY_AND_DISK_SER)
 * 
 * TODO：
 * 1.对于不同类型的Block，是否应该采用不同的存取策略。
 * 目前做法：通过修改存储级别来满足，Shared memory中只存储RDD类型的Block
 * 
 * 2.现在只针对RDD block，对于Broadcast类型的block，一般大小比较小，仍旧存储到本地内存中，方便使用。
 * 目前做法：其他类型的数据仍然使用本地内存
 * 
 * 3.所以存储Block时候，需要先判断是否是RDD类型的Block。
 * 目前做法：可以满足
 * 
 * 4.进行存储的时候，先确定Block不存在
 * 目前做法：这个是由外部调用流程实现，先看Block是否存在，不存在才会引起一个缓存的过程
 * 读Block时候先调用了contains方法，如果contains存在，则调用getBytes方法返回数据
 * 
 */
class LocalMemoryStore(
    blockManager: BlockManager, 
    maxMemory: Long, 
    serverClient: BlockServerClient) 
  extends BlockStore(blockManager) with Logging {
  
  logInfo("Shared memory Store started")
  
  /**
   * 本地Block列表，在新增Block成功时，新增本地Block
   * 查询时，优先查找本地block列表
   * 在应用程序结束时候，删除本地block列表，同时通知删除BlockServerWorker上属于本client的Block列表
   * 有多线程的并发访问
   */
  val entries = new ConcurrentHashMap[SBlockId, SBlockEntry]
  
  val local2SBlock = new HashMap[BlockId, SBlockId]
  
  /**
   * TODO：赋值RDD的血统信息
   */
  private def getSBlockId(blockId: BlockId) = {
    local2SBlock.synchronized {
      if (!local2SBlock.containsKey(blockId)) {
        local2SBlock.put(blockId, SBlockId(blockId))
      }
      local2SBlock.get(blockId)
    }
  }
  
  override def contains(blockId: BlockId): Boolean = {
    fetchBlockIfNotExist(SBlockId(blockId)) != null
  }
  
  /**
   * 
   * 调用时候先调用contains方法，保证block存在
   */
  override def getSize(blockId: BlockId): Long = {
    //fetchBlockIfNotExist(SBlockId(blockId))
    entries.get(SBlockId(blockId)).size
  }
  
  /**
   * 如果本地Block映射不存在，则去worker节点进行查询。
   * 查询不到的场合比较少，因为大多数block都是本Executor来进行注册的，本地列表中都存在缓存。
   * 改进并发访问效率
   */
  private def fetchBlockIfNotExist(blockId: SBlockId) = {
    var entry: SBlockEntry = null
    if (!entries.containsKey(blockId)) {
      logInfo("Local block not exist. fectch from Shared memory.")
      serverClient.getBlock(blockId).map { res =>
        logInfo("Fectch block from Shared memory success, put it into local block data manager.")
        entry = res
        entries.put(blockId, entry)
      }

    } else {
      entry = entries.get(blockId)
    }
    entry
  }
  
  //已经确定block不存在
  override def putBytes(blockId: BlockId, bytes: ByteBuffer, level: StorageLevel): PutResult = {
    putIntoSharedMemoryStore(blockId, bytes, returnValues = true)
  }

  override def putArray(
      blockId: BlockId,
      values: Array[Any],
      level: StorageLevel,
      returnValues: Boolean): PutResult = {
    putIterator(blockId, values.toIterator, level, returnValues)
  }

  /**
   * 将iterator[T]对象放入到共享存储中，一般是已经得到数据的对象数组
   * 这样才可以序列化
   * 
   * dataSerialize是一次性完成，把对象数组都写入到输出流中，然后放到对象数组中
   * dataDeserialize可以先把ByteBuffer包装成InputStream，然后每次读取一个对象数据
   * 
   * 
   */
  override def putIterator(
      blockId: BlockId,
      values: Iterator[Any],
      level: StorageLevel,
      returnValues: Boolean): PutResult = {
    logDebug(s"Attempting to write values for block $blockId")
    //必须先序列化成Byte数组
    val bytes = blockManager.dataSerialize(blockId, values)
    putIntoSharedMemoryStore(blockId, bytes, returnValues)
  }

  /**
   * 将Byte数组存放到共享存储中
   * 
   * TODO：****能否在序列化的同时就写入到共享存储中，减少数据的拷贝开销？可以改动下序列化方法，直接序列化共享内存中
   * 但是Put的同时，一般需要返回数组以供本次计算使用，直接写入的话本次计算则不能使用
   */
  private def putIntoSharedMemoryStore(
      blockId: BlockId,
      bytes: ByteBuffer,
      returnValues: Boolean): PutResult = {
    // So that we do not modify the input offsets !
    // duplicate does not copy buffer, so inexpensive
    val sid = SBlockId(blockId)
    val byteBuffer = bytes.duplicate()
    byteBuffer.rewind()
    //TODO: Info to Debug
    logInfo(s"Attempting to put block $sid into SharedMemoryStore")
    val startTime = System.currentTimeMillis
    //TODO: ****这里也需要做一定的抽象，将数据方便的放入到共享存储中，对上层提供一个清晰的API
    //先与worker通信，申请空间等工作，然后再写入
    //TODO: ****name怎么确定
    var os: LocalBlockOutputStream = null
    //TODO：访问worker节点，请求分配空间，这里应该传入唯一的共享id，userDefinedId（参见SBlockId的解释）
    serverClient.reqNewBlock(sid.userDefinedId, byteBuffer.limit()) match {
      case Some(entry) => 
        var success = true
        try {
          //TODO: 远程写
          os = LocalBlockOutputStream.getLocalOutputStream("shmget", entry.entryId, byteBuffer.limit())
          //os = BlockOutputStream("shmget", entry.entryStr)
          os.write(byteBuffer.array())
        } catch {
          case ioe: IOException =>
            logWarning(s"Failed to write the block $sid to SharedMemoryStore", ioe)
            success = false
        } finally {
          os.close()
        }
        
        //success=false有两种情况：1.服务器返回空间不足。2.写文件出错。对于第二种情况，需要对worker进行写结果，成功或失败
        //TODO：客户端如果有一个线程任务正在申请，则另一个线程应该等待写完
        logInfo(s"Block $sid Write share memory $success, now writeBlockResult to BlockServerWorker")
        serverClient.writeBlockResult(entry.entryId, success) match {
          //成功写入, newSid: userDefinedId not empty, localBlockId empty, name not empty
          case Some(newSid) =>
            assert(sid.equals(newSid))
            sid.name = newSid.name
            entries.put(sid, entry)
          case None =>
            logWarning(s"Cannot apply enough shared memory space for block $sid")
        }
      //如果不能申请到足够的空间
      case None =>
        logWarning(s"Cannot apply enough shared memory space for block $sid")
    }
    
    
    //val file = tachyonManager.getFile(blockId)
    //val os = file.getOutStream(WriteType.TRY_CACHE)

    
    val finishTime = System.currentTimeMillis
    //TODO: Info to Debug
    logInfo("Block %s stored as %s file in SharedMemoryStore in %d ms".format(
      blockId, Utils.bytesToString(byteBuffer.limit), finishTime - startTime))

    if (returnValues) {
      PutResult(bytes.limit(), Right(bytes.duplicate()))
    } else {
      PutResult(bytes.limit(), null)
    }
  }

  override def remove(blockId: BlockId): Boolean = {
    
    val entry = entries.remove(SBlockId(blockId))
    if (entry != null) {
      logInfo(s"Block $blockId of size ${entry.size} dropped from memory")
      serverClient.removeBlock(SBlockId(blockId))
      true
    } else {
      false
    }
    
  }

  /**
   * Resolved：与BlockManager的功能相结合，是否getValues调用之前先调用contains方法？
   */
  override def getValues(blockId: BlockId): Option[Iterator[Any]] = {
    val sid = SBlockId(blockId)
    val entry = fetchBlockIfNotExist(sid)
    if (entry == null || entry.size <= 0) {
      logWarning(s"request block $sid from SharedMemoryStore doesn't have or no content")
      None
    }
    
    var is: LocalBlockInputStream = null
    if (entry.local) {
      is = LocalBlockInputStream.getLocalInputStream("shmget", entry.entryId, entry.size.toInt)
    } else {//远程Block
      //is = 
    }
    
    assert (is != null)
    try {
      val bs = is.readFully(entry.size.toInt)
      //TODO：增加blockManager.dataDeserialize方法对于inputStream的支持
      //目前做法是先全部读取到堆内存，然后再返回给上层调用
      return Some(blockManager.dataDeserialize(blockId, ByteBuffer.wrap(bs)))
    } catch {
      case ioe: IOException =>
        logWarning(s"Failed to fetch the block $blockId from SharedMemoryStore", ioe)
        None
    } finally {
      //is.close()
    }
  }

  override def getBytes(blockId: BlockId): Option[ByteBuffer] = {
    
    val sid = SBlockId(blockId)
    val entry = fetchBlockIfNotExist(sid)
    if (entry == null || entry.size <= 0) {
      logWarning(s"request block $sid from SharedMemoryStore doesn't have or no content")
      None
    }
    
    var is: LocalBlockInputStream = null
    if (entry.local) {
      is = LocalBlockInputStream.getLocalInputStream("shmget", entry.entryId, entry.size.toInt)
    } else {//远程Block
      //is = 
    }
    
    assert (is != null)
    try {
      val size = entry.size
      //val bs = new Array[Byte](size.toInt)
      //这里修改为一次性读出Array，返回
      val bs = is.readFully(size.toInt)
      //ByteStreams.readFully(is, bs) //TODO有一次内存拷贝，这里是否可以消除？
      Some(ByteBuffer.wrap(bs))
    } catch {
      case ioe: IOException =>
        logWarning(s"Failed to fetch the block $blockId from SharedMemoryStore", ioe)
        None
    } finally {
      is.close()
    }
  }
  
  /**
   * 结束Executor时候的清理工作
   */
  override def clear() {
    serverClient.unregisterClient()
  }

}
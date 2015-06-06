/**
 *
 */
package org.apache.spark.smstorage

import org.scalatest.FunSuite
import org.apache.spark.SparkConf
import akka.actor.ActorSystem
import org.apache.spark.smstorage.client.BlockServerClient
import org.apache.spark.util.AkkaUtils
import org.apache.spark.SecurityManager
import org.apache.spark.smstorage.worker.BlockServerWorkerActor
import org.apache.spark.smstorage.worker.SpaceManager
import org.apache.spark.smstorage.sharedmemory.SMemoryManager
import org.apache.spark.smstorage.worker.BlockIndexer
import org.apache.spark.storage.BlockId
import org.apache.spark.storage.RDDBlockId
import org.scalatest.BeforeAndAfter
import akka.actor.ActorRef
import akka.actor.Props
import org.apache.spark.storage.BlockManagerMasterActor
import org.apache.spark.smstorage.client.BlockServerClient
import org.apache.spark.smstorage.client.io.LocalBlockOutputStream
import java.nio.ByteBuffer
import org.apache.spark.smstorage.client.io.LocalBlockInputStream
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import org.apache.spark.deploy.worker.Worker
import org.apache.spark.util.Utils

/**
 * @author hwang
 * TODO：模拟客户端Executor建立连接，然后进行并发的读写
 */
class BlockServerWorkerSuite extends FunSuite with BeforeAndAfter {
  
  val conf: SparkConf = new SparkConf(false)
  
  var actorSystem: ActorSystem = null
  
  val MB = 1024*1024
  
  var worker: ActorRef = null
  var client: BlockServerClient = null
  
  //var writeBlockId: SBlockId = null
  
  test("BlockId to SBlockId") {
    val blockId = new RDDBlockId(1, 2)
    val sblockId = SBlockId(blockId)//转换成的SBlock name为空
    
    assert(sblockId.name.isEmpty())
    assert(!sblockId.userDefinedId.isEmpty())
    assert(sblockId.localBlockId === "rdd_1_2")
    
    //assert(sblockId == SBlockId())
    
    
    val blockId2 = new RDDBlockId(1, 2, "KMeansInput")
    
    val sblockId2 = SBlockId(blockId2)
    assert(sblockId != sblockId2)
    
    val sblockId3 = new SBlockId("KMeansInput")
    assert(sblockId3 == sblockId2)
    
    
  }
  before {
    val securityManager: SecurityManager = new SecurityManager(conf)
    val hostname = "localhost"
    val (actorSystem, port) = AkkaUtils.createActorSystem("test", hostname, 0, conf, securityManager)
    this.actorSystem = actorSystem
    
    val deamonWorker: Worker = null
    worker = actorSystem.actorOf(Props(new BlockServerWorkerActor(conf, deamonWorker)), "worker")
    val clientId: BlockServerClientId = new BlockServerClientId("test", "localhost", 9999)
    client = new BlockServerClient(clientId, worker, conf)
    val jvmId = Utils.getJvmId()
    println(jvmId)
    client.registerClient(20*MB, jvmId, null)
  }
  after {
    actorSystem.shutdown()
    actorSystem.awaitTermination()
    actorSystem = null
    worker = null
  }
  
  test("test worker actor start") {

    
//    val smManager: SMemoryManager = new SMemoryManager()
//    val spaceManager: SpaceManager = new SpaceManager(20000, smManager)
//    val blockIndexer: BlockIndexer = new BlockIndexer()
//    val worker: BlockServerWorkerActor = new BlockServerWorkerActor(conf, spaceManager, blockIndexer)
    
    //assert(client.reqNewBlock("srdd01", 100).isEmpty)
    //client.registerClient(15*MB, null)

  }
  
  test("test worker actor write block") {
    
    val blockId = new RDDBlockId(2, 1, "KMeansInput|"+1)
    val sblockId = SBlockId(blockId)
    
    assert(client.getBlock(sblockId).isEmpty)
    //assert(client.getBlockSize(sblockId))
    
    val userDefinedId = sblockId.userDefinedId;
    assert(userDefinedId == "KMeansInput|"+1)

    assert(client.reqNewBlock(userDefinedId, MB*20).isEmpty)
    
    val size=2*MB
    val res = client.reqNewBlock(userDefinedId, size)
    assert(res.isDefined)
    
    val entry=res.get
    
    val byteArr = new Array[Byte](size)
    var i=0
    while (i<byteArr.length) {
      byteArr(i)=123
      i=i+1
    }
    byteArr(0)=122
    byteArr(byteArr.length-1)=124
    
    val byteBuffer=ByteBuffer.wrap(byteArr)
    
    SMStorageWriteTest.printByteArr(byteArr, 100)
    SMStorageWriteTest.printByteArrLast(byteArr, 100)
    
    val os = LocalBlockOutputStream.getLocalOutputStream("shmget", entry.entryId, byteBuffer.limit())
    os.write(byteBuffer.array())
    os.close()
    
    val newblockid = client.writeBlockResult(entry.entryId, true)
    assert(newblockid.isDefined)
    assert(newblockid.get.userDefinedId == sblockId.userDefinedId)

    //这里测试写入Block之后再读取Block
    //客户端的Block：userDefinedId=localBlockId="rdd_1_2", name=""
    //而worker端服务器中的block：userDefinedId="rdd_1_2", name="1474577", localBlockId=""
    //需要修改匹配策略，否则两者的Block匹配不到
    val writeBlockId = newblockid.get
    val getEntry = client.getBlock(writeBlockId)
    
    assert(getEntry.isDefined)
    assert(getEntry.get.entryId == entry.entryId)
    assert(getEntry.get.local==true)
    assert(getEntry.get.size==size)
    
    
    val entry2 = client.getBlock(writeBlockId).get
    println(entry2.entryId)
    
    var is: LocalBlockInputStream = null
    if (entry2.local) {
      is = LocalBlockInputStream.getLocalInputStream("shmget", entry2.entryId, entry2.size.toInt)
    } else {//远程Block
      //is = 
    }
    assert(is != null)
    val bs = is.readFully(entry2.size.toInt)
    is.close()
    SMStorageWriteTest.printByteArr(bs, 100)
    SMStorageWriteTest.printByteArrLast(bs, 100)    
    
  }
  
  test("test worker concurrent write read block") {
    //测试并发的读写共享内存
    //已测试申请200个共享内存，测试成功
    val num = 8//申请8*2=16MB超过15MB，则第八个申请时会失败
    //val pool = Executors.newFixedThreadPool(num) //使用Executors，线程的异常会被隐藏，不向上抛出
    for (i <- 1 to num) {

      new Thread(new Runnable() {
        def run() {
          testConcurrentReadWrite(i)
        }
      }).run()

    }
    Thread.sleep(10000)
    //pool.awaitTermination(20, TimeUnit.SECONDS)
  }
  
  test("test worker once write mutil read block") {
    //写一次RDD，使用多个线程来模拟并发的读
    val sblockId = testWriteBlock(10, 2*MB)
    
    val num=7
    for (i <- 1 to num) {

      new Thread(new Runnable() {
        def run() {
          testReadBlock(sblockId, 10)
        }
      }).run()

    }
    Thread.sleep(10000)
  }
  
  def testWriteBlock(index: Int, size: Int=2*MB) = {
    val blockId = new RDDBlockId(1, index)
    val sblockId = SBlockId(blockId)
    
    assert(client.getBlock(sblockId).isEmpty)
    //assert(client.getBlockSize(sblockId))
    
    val userDefinedId = sblockId.userDefinedId;
    assert(userDefinedId == "rdd_1_"+index)
    println(sblockId.userDefinedId+" thread begin write")
    //assert(client.reqNewBlock(userDefinedId, MB*20).isEmpty)
    
    val size=2*MB
    val res = client.reqNewBlock(userDefinedId, size)
    assert(res.isDefined)
    
    val entry=res.get
    
    val byteArr = new Array[Byte](size)
    var i=0
    while (i<byteArr.length) {
      byteArr(i)=100
      i=i+1
    }
    byteArr(0)=(100+index-1).toByte
    byteArr(byteArr.length-1)=(100+index+1).toByte
    
    val byteBuffer=ByteBuffer.wrap(byteArr)
    
    SMStorageWriteTest.printByteArr(byteArr, 100)
    SMStorageWriteTest.printByteArrLast(byteArr, 100)
    
    val os = LocalBlockOutputStream.getLocalOutputStream("shmget", entry.entryId, byteBuffer.limit())
    os.write(byteBuffer.array())
    os.close()
    
    val newblockid = client.writeBlockResult(entry.entryId, true)
    assert(newblockid.isDefined)
    assert(newblockid.get.userDefinedId == sblockId.userDefinedId)
    
    newblockid.get
  }
  
  def testReadBlock(sblockId: SBlockId, index: Int) {
    
    val entry2 = client.getBlock(sblockId).get
    println(entry2.entryId)
    
    var is: LocalBlockInputStream = null
    if (entry2.local) {
      is = LocalBlockInputStream.getLocalInputStream("shmget", entry2.entryId, entry2.size.toInt)
    } else {//远程Block
      //is = 
    }
    assert(is != null)
    val bs = is.readFully(entry2.size.toInt)
    is.close()

    assert(bs(0)==(100+index-1).toByte)
    assert(bs(bs.length-1)==(100+index+1).toByte)
    
    SMStorageWriteTest.printByteArr(bs, 100)
    SMStorageWriteTest.printByteArrLast(bs, 100)    
  }
  
  def testConcurrentReadWrite(index: Int) {
    val blockId = new RDDBlockId(1, index)
    val sblockId = SBlockId(blockId)
    
    assert(client.getBlock(sblockId).isEmpty)
    //assert(client.getBlockSize(sblockId))
    
    val userDefinedId = sblockId.userDefinedId;
    assert(userDefinedId == "rdd_1_"+index)
    println(sblockId.userDefinedId+" thread begin write")
    //assert(client.reqNewBlock(userDefinedId, MB*20).isEmpty)
    
    val size=2*MB
    val res = client.reqNewBlock(userDefinedId, size)
    assert(res.isDefined)
    
    val entry=res.get
    
    val byteArr = new Array[Byte](size)
    var i=0
    while (i<byteArr.length) {
      byteArr(i)=100
      i=i+1
    }
    byteArr(0)=(100+index-1).toByte
    byteArr(byteArr.length-1)=(100+index+1).toByte
    
    val byteBuffer=ByteBuffer.wrap(byteArr)
    
    //SMStorageWriteTest.printByteArr(byteArr, 100)
    //SMStorageWriteTest.printByteArrLast(byteArr, 100)
    
    val os = LocalBlockOutputStream.getLocalOutputStream("shmget", entry.entryId, byteBuffer.limit())
    os.write(byteBuffer.array())
    os.close()
    
    val newblockid = client.writeBlockResult(entry.entryId, true)
    assert(newblockid.isDefined)
    assert(newblockid.get.userDefinedId == sblockId.userDefinedId)

    //这里测试写入Block之后再读取Block
    //客户端的Block：userDefinedId=localBlockId="rdd_1_2", name=""
    //而worker端服务器中的block：userDefinedId="rdd_1_2", name="1474577", localBlockId=""
    //需要修改匹配策略，否则两者的Block匹配不到
    val writeBlockId = newblockid.get
    val getEntry = client.getBlock(writeBlockId)
    
    assert(getEntry.isDefined)
    assert(getEntry.get.entryId == entry.entryId)
    assert(getEntry.get.local==true)
    assert(getEntry.get.size==size)
    
    
    val entry2 = client.getBlock(writeBlockId).get
    println(entry2.entryId)
    
    var is: LocalBlockInputStream = null
    if (entry2.local) {
      is = LocalBlockInputStream.getLocalInputStream("shmget", entry2.entryId, entry2.size.toInt)
    } else {//远程Block
      //is = 
    }
    assert(is != null)
    val bs = is.readFully(entry2.size.toInt)
    is.close()
    SMStorageWriteTest.printByteArr(bs, 100)
    SMStorageWriteTest.printByteArrLast(bs, 100)    
  }
  

}
/**
 *
 */
package org.apache.spark.smstorage

import org.apache.spark.network.buffer.NioManagedBuffer
import org.apache.spark.smstorage.BlockServerMessages.MigrateDestination
import org.mockito.Mock
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
import org.apache.spark.storage._
import org.scalatest.BeforeAndAfter
import akka.actor.ActorRef
import akka.actor.Props
import org.apache.spark.smstorage.client.BlockServerClient
import org.apache.spark.smstorage.client.io.LocalBlockOutputStream
import java.nio.ByteBuffer
import org.apache.spark.smstorage.client.io.LocalBlockInputStream
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import org.apache.spark.deploy.worker.Worker
import org.apache.spark.util.Utils

import org.mockito.Mockito.{mock, when}

/**
 * @author Wang Haihua
 * 注意：对于每一个测试，需要基于linux系统，且指定以下JVM参数，便于加载native库文件！！！
 * Need specify JVM parametre before test:
 * -Djava.library.path=/home/hadoop/develop/spark/lib/native/
 *
 */
class BlockServerWorkerSuite extends FunSuite with BeforeAndAfter {
  
  val conf: SparkConf = new SparkConf(false)
  
  var actorSystem: ActorSystem = null
  
  val MB = 1024*1024
  
  var workerActor: ActorRef = null
  var worker: BlockServerWorkerActor = null
  var client: BlockServerClient = null

  val transferServicePort = 8975

  val initCachedSpace = 20 * MB
  
  //var writeBlockId: SBlockId = null
  
  before {

    conf.set("spark.smspark.initcachedspace", initCachedSpace.toString)
    conf.set("spark.smspark.blockTransferService", "nio")
    conf.set("spark.smspark.blockTransferService", "nio")
    conf.set("spark.blockManager.port", transferServicePort.toString)

    System.setProperty("java.library.path", "/home/hadoop/develop/spark/lib/native/");
    System.load("/home/hadoop/develop/spark/lib/native/ShmgetAccesser.so");//访问共享内存的动态链接库

    println("******************before test.")
    val securityManager: SecurityManager = new SecurityManager(conf)
    val hostname = "localhost"
    val (actorSystem, port) = AkkaUtils.createActorSystem("test", hostname, 0, conf, securityManager)
    this.actorSystem = actorSystem
    
    val deamonWorker: Worker = mock(classOf[Worker])
    when(deamonWorker.securityMgr).thenReturn(securityManager)
    when(deamonWorker.memory).thenReturn(20*2) //40MB
    //worker = new BlockServerWorkerActor(conf, deamonWorker)
    //workerActor = actorSystem.actorOf(Props(worker), "worker")
    workerActor = actorSystem.actorOf(Props(new BlockServerWorkerActor(conf, deamonWorker)), "worker")

    val clientId: BlockServerClientId = new BlockServerClientId("test", "localhost", 9999, "test-app")
    client = new BlockServerClient(clientId, workerActor, conf)
    val jvmId = Utils.getJvmId()
    println(jvmId)
    client.registerClient(512*MB, 20*MB, jvmId, null)

    worker = SMSparkContext.bsWorker
    println(worker != null)
  }
  after {
    println("******************after test.")
    client.unregisterClient()

    //actorSystem.shutdown()
    actorSystem.awaitTermination()
    //actorSystem = null
    //worker = null
  }
  
  test("test worker actor start") {

    
//    val smManager: SMemoryManager = new SMemoryManager()
//    val spaceManager: SpaceManager = new SpaceManager(20000, smManager)
//    val blockIndexer: BlockIndexer = new BlockIndexer()
//    val worker: BlockServerWorkerActor = new BlockServerWorkerActor(conf, spaceManager, blockIndexer)
    
    //assert(client.reqNewBlock("srdd01", 100).isEmpty)
    //client.registerClient(15*MB, null)

  }

  private def tryWriteBlock(size: Long): Unit = {

  }
  
  test("test worker actor write block") {

    println("Test worker actor write block begin...")
    
    val blockId = new RDDBlockId(2, 1, "KMeansInput|"+1)
    val sblockId = SBlockId(blockId)
    
    assert(client.getBlock(sblockId).isEmpty)
    //assert(client.getBlockSize(sblockId))
    
    val userDefinedId = sblockId.userDefinedId
    assert(userDefinedId == "KMeansInput|"+1)

    //TODO: Request block size > totalSpaceSize, return false directly.
    //assert(client.reqNewBlock(userDefinedId, MB*30).isEmpty)
    
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

    //TimeUnit.SECONDS.sleep(30)
    
  }

  test("test RDDBlock write without userDefinedId") {

    val blockId = new RDDBlockId(2, 1)
    val sblockId = SBlockId(blockId)

    assert(client.getBlock(sblockId).isEmpty)
    //assert(client.getBlockSize(sblockId))

    val userDefinedId = sblockId.userDefinedId;
    assert(userDefinedId == "rdd_2_1")

    assert(client.reqNewBlock(userDefinedId, MB*21).isEmpty)

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

    //TimeUnit.SECONDS.sleep(30)

  }

  test("test worker write read block") {

  }

  /**
   * Main test read/write block
   */
  test("test worker concurrent write read block") {
    //测试并发的读写共享内存
    //已测试申请200个共享内存，测试成功
    val num = 20//申请8*2=16MB超过15MB，则第八个申请时会失败
    //val pool = Executors.newFixedThreadPool(num) //使用Executors，线程的异常会被隐藏，不向上抛出
    for (i <- 1 to num) {

      new Thread(new Runnable() {
        def run() {
          testConcurrentReadWrite(i)
        }
      }).run()

    }

    //Test write block belong to same RDD
    var size=20*MB
    var res = client.reqNewBlock("app#20#21", size)
    assert(res.isEmpty)

    //Test write block size > total Cached Space memory size
    size=21*MB
    res = client.reqNewBlock("app#21#21", size)
    assert(res.isEmpty)

    //testConcurrentReadWrite(21)

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

  test("test block data manager function") {
    //上传一个block，然后再读出一个block
    assert(worker != null)
    val index = 89
    val data = new NioManagedBuffer(generateBytesData(index, 10*MB))
    val blockId = new TestBlockId(s"app#$index#1")

    SMStorageWriteTest.printByteArr(data.nioByteBuffer().array(), 100)
    SMStorageWriteTest.printByteArrLast(data.nioByteBuffer().array(), 100)

    worker.putBlockData(blockId, data, StorageLevel.OFF_HEAP)
    val dataOut = worker.getBlockData(blockId)

    SMStorageWriteTest.printByteArr(dataOut.nioByteBuffer().array(), 100)
    SMStorageWriteTest.printByteArrLast(dataOut.nioByteBuffer().array(), 100)
  }

  test("test block transfer") {
    val index = 99

    //远程迁移一个Block，然后再下载
    val globalId = s"app#$index#1"
    val data = new NioManagedBuffer(generateBytesData(index, 10*MB))
    val blockId = new TestBlockId(globalId)

    SMStorageWriteTest.printByteArr(data.nioByteBuffer().array(), 100)
    SMStorageWriteTest.printByteArrLast(data.nioByteBuffer().array(), 100)

    val target = MigrateDestination("localWorker", "127.0.0.1", transferServicePort)
    //workerActor

    worker.blockTransferService.uploadBlockSync(
      target.host,
      target.port,
      "",
      blockId,
      data,
      StorageLevel.OFF_HEAP
    )

    val dataDownload = worker.blockTransferService.fetchBlockSync(
      target.host, target.port, "localWorker", blockId.toString)

    SMStorageWriteTest.printByteArr(dataDownload.nioByteBuffer().array(), 100)
    SMStorageWriteTest.printByteArrLast(dataDownload.nioByteBuffer().array(), 100)
  }

  def generateBytesData(index: Int, size: Int) = {
    val byteArr = new Array[Byte](size)
    var i=0
    while (i<byteArr.length) {
      byteArr(i)=100
      i=i+1
    }
    byteArr(0)=(100+index-1).toByte
    byteArr(byteArr.length-1)=(100+index+1).toByte

    val byteBuffer=ByteBuffer.wrap(byteArr)
    byteBuffer
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

    val byteBuffer = generateBytesData(index, size)
    
    SMStorageWriteTest.printByteArr(byteBuffer.array(), 100)
    SMStorageWriteTest.printByteArrLast(byteBuffer.array(), 100)
    
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
    val globalId = SBlockId.mkGlobalBlockId2("app", index, index)
    val blockId = new RDDBlockId(index, index, globalId)
    val sblockId = SBlockId(blockId)
    
    assert(client.getBlock(sblockId).isEmpty)
    //assert(client.getBlockSize(sblockId))
    
    val userDefinedId = sblockId.userDefinedId
    assert(userDefinedId == s"app#"+index+"#"+index)
    println(sblockId.userDefinedId + " thread begin write")
    //assert(client.reqNewBlock(userDefinedId, MB*20).isEmpty)
    
    val size=index*MB
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
    println(newblockid+"has been write successfully")
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
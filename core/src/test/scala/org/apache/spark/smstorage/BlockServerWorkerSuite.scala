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

/**
 * @author hwang
 *
 */
class BlockServerWorkerSuite extends FunSuite with BeforeAndAfter {
  
  val conf: SparkConf = new SparkConf(false)
  
  var actorSystem: ActorSystem = null
  
  val MB = 1024*1024
  
  var worker: ActorRef
  var client: BlockServerClient
  
  test("BlockId to SBlockId") {
    val blockId = new RDDBlockId(1, 2)
    val sblockId = SBlockId(blockId)//转换成的SBlock name为空
    
    assert(sblockId.name.isEmpty())
    assert(!sblockId.userDefinedId.isEmpty())
    assert(sblockId.localBlockId === "rdd_1_2")
    
    //assert(sblockId == SBlockId())
    val sblockId2 = SBlockId(blockId, "srdd_1_2")
    assert(sblockId != sblockId2)
    
    val sblockId3 = new SBlockId("srdd_1_2")
    assert(sblockId3 == sblockId2)
    
    
  }
  before {
    val securityManager: SecurityManager = new SecurityManager(conf)
    val hostname = "localhost"
    val (actorSystem, port) = AkkaUtils.createActorSystem("test", hostname, 0, conf, securityManager)
    this.actorSystem = actorSystem
    
    val smManager: SMemoryManager = new SMemoryManager()
    val spaceManager: SpaceManager = new SpaceManager(1024*1024*10, smManager)
    val blockIndexer: BlockIndexer = new BlockIndexer()
    
    worker = actorSystem.actorOf(Props(new BlockServerWorkerActor(conf, spaceManager, blockIndexer)), "worker")
    val clientId: BlockServerClientId = new BlockServerClientId("test", "localhost", 9999)
    client = new BlockServerClient(clientId, worker, conf)
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
    
    assert(client.reqNewBlock("srdd01", 100).isEmpty)
    client.registerClient(20000, null)
    
    
    val blockId = new RDDBlockId(1, 2)
    val sblockId = SBlockId(blockId)
    
    assert(client.getBlock(sblockId) == null)
    //assert(client.getBlockSize(sblockId))
    
    val userDefinedId = sblockId.userDefinedId;
    assert(userDefinedId == "rdd_1_2")

    assert(client.reqNewBlock(userDefinedId, MB*20).isEmpty)
    
    val size=2*MB
    val res = client.reqNewBlock(userDefinedId, size)
    assert(res.isDefined)
    
    val entry=res.get
    
    val byteArr = new Array[Byte](size)
    var i=0
    while (i<byteArr.length) {
      byteArr(i)=123
    }
    byteArr(0)=122
    byteArr(byteArr.length-1)
    
    val byteBuffer=ByteBuffer.wrap(byteArr)
    
    //SMStorageWriteTest
    
    val os = LocalBlockOutputStream.getLocalOutputStream("shmget", entry.entryId, byteBuffer.limit())
    os.write(byteBuffer.array())
    os.close()
    
    val blockid = client.writeBlockResult(entry.entryId, true)
    assert(blockid.isDefined)

    val getEntry = client.getBlock(sblockId)
    assert(getEntry != null)
    assert(getEntry.entryId == entry.entryId)
    assert(getEntry.local==true)
    assert(getEntry.size==size)
  }
  
  test("test worker actor write block") {
    
    val blockId = new RDDBlockId(1, 2)
    val sblockId = SBlockId(blockId)
    
    assert(client.getBlock(sblockId) == null)
    //assert(client.getBlockSize(sblockId))
    
    val userDefinedId = sblockId.userDefinedId;
    assert(userDefinedId == "rdd_1_2")

    assert(client.reqNewBlock(userDefinedId, MB*20).isEmpty)
    
    val size=2*MB
    val res = client.reqNewBlock(userDefinedId, size)
    assert(res.isDefined)
    
    val entry=res.get
    
    val byteArr = new Array[Byte](size)
    var i=0
    while (i<byteArr.length) {
      byteArr(i)=123
    }
    byteArr(0)=122
    byteArr(byteArr.length-1)
    
    val byteBuffer=ByteBuffer.wrap(byteArr)
    
    //SMStorageWriteTest
    
    val os = LocalBlockOutputStream.getLocalOutputStream("shmget", entry.entryId, byteBuffer.limit())
    os.write(byteBuffer.array())
    os.close()
    
    val blockid = client.writeBlockResult(entry.entryId, true)
    assert(blockid.isDefined)

    val getEntry = client.getBlock(sblockId)
    assert(getEntry != null)
    assert(getEntry.entryId == entry.entryId)
    assert(getEntry.local==true)
    assert(getEntry.size==size)
  }
  
  test("test worker actor write read Block") {
    //val securityManager: SecurityManager = new SecurityManager(conf)
    //val hostname = "localhost"
    //val (actorSystem, port) = AkkaUtils.createActorSystem("test", hostname, 0, conf, securityManager)
    
//    val smManager: SMemoryManager = new SMemoryManager()
//    val spaceManager: SpaceManager = new SpaceManager(20000, smManager)
//    val blockIndexer: BlockIndexer = new BlockIndexer()
//    val worker: BlockServerWorkerActor = new BlockServerWorkerActor(conf, spaceManager, blockIndexer)
//    
//    val clientId: BlockServerClientId = new BlockServerClientId("test", "localhost", 9999)
    
    
    //client.reqNewBlock(userDefinedId, size);
    
    //assert(worker.registerClient(clientId, 20000, null), "")
    
    
  }
  
  

}
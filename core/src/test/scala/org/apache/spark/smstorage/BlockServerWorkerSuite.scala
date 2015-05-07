/**
 *
 */
package org.apache.spark.smstorage

import org.scalatest.FunSuite
import org.apache.spark.SparkConf
import org.scalatest.BeforeAndAfterAll
import akka.actor.ActorSystem
import org.apache.spark.smstorage.client.BlockServerClient
import org.apache.spark.util.AkkaUtils
import org.apache.spark.SecurityManager
import org.apache.spark.smstorage.worker.BlockServerWorkerActor
import org.apache.spark.smstorage.worker.BlockServerWorkerActor
import org.apache.spark.smstorage.worker.SpaceManager
import org.apache.spark.smstorage.worker.SpaceManager
import org.apache.spark.smstorage.sharedmemory.SMemoryManager
import org.apache.spark.smstorage.worker.BlockIndexer
import org.apache.spark.storage.BlockId
import org.apache.spark.storage.RDDBlockId

/**
 * @author hwang
 *
 */
class BlockServerWorkerSuite extends FunSuite with BeforeAndAfterAll {
  
  val conf: SparkConf = new SparkConf(false)
  
  var actorSystem: ActorSystem = null
  
  test("BlockId to SBlockId") {
    val blockId = new RDDBlockId(1, 2)
    val sblockId = SBlockId(blockId)//转换成的SBlock name为空
    
    assert(sblockId.name == null)
    assert(sblockId.userDefinedId != null)
    assert(sblockId.localBlockId === "rdd_1_2")
    
    //assert(sblockId == SBlockId())
    val sblockId2 = SBlockId(blockId, "srdd_1_2")
    assert(sblockId != sblockId2)
    
    val sblockId3 = new SBlockId("srdd_1_2")
    assert(sblockId3 == sblockId)
    
    
  }
  
  test("test worker actor start") {
    //val securityManager: SecurityManager = new SecurityManager(conf)
    //val hostname = "localhost"
    //val (actorSystem, port) = AkkaUtils.createActorSystem("test", hostname, 0, conf, securityManager)
    
    val smManager: SMemoryManager = new SMemoryManager()
    val spaceManager: SpaceManager = new SpaceManager(20000, smManager)
    val blockIndexer: BlockIndexer = new BlockIndexer()
    val worker: BlockServerWorkerActor = new BlockServerWorkerActor(conf, spaceManager, blockIndexer)
    
    val clientId: BlockServerClientId = new BlockServerClientId("test", "localhost", 9999)
    
    assert(worker.reqNewBlock(clientId, "srdd01", 100).isEmpty)
    
    assert(worker.registerClient(clientId, 20000, null), "")
    assert(!worker.registerClient(clientId, 20000, null), "")
    
    
  }
  
  test("test worker actor write read Block") {
    //val securityManager: SecurityManager = new SecurityManager(conf)
    //val hostname = "localhost"
    //val (actorSystem, port) = AkkaUtils.createActorSystem("test", hostname, 0, conf, securityManager)
    
    val smManager: SMemoryManager = new SMemoryManager()
    val spaceManager: SpaceManager = new SpaceManager(20000, smManager)
    val blockIndexer: BlockIndexer = new BlockIndexer()
    val worker: BlockServerWorkerActor = new BlockServerWorkerActor(conf, spaceManager, blockIndexer)
    
    val clientId: BlockServerClientId = new BlockServerClientId("test", "localhost", 9999)
    
    
    
    //assert(worker.registerClient(clientId, 20000, null), "")
    
    
  }
  
  

}
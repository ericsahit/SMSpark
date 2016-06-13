/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.deploy.master

import scala.collection.mutable
import akka.actor.ActorRef
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.util.Utils
import org.apache.spark.smstorage.SBlockId
import org.apache.spark.smstorage.SBlockEntry
import org.apache.spark.smstorage.SBlockEntry
import org.apache.spark.smstorage.SBlockEntry
import org.apache.spark.smstorage.SBlockEntry

private[spark] class WorkerInfo(
    val id: String,
    val host: String,
    val port: Int,
    val cores: Int,
    val memory: Int,
    val actor: ActorRef,
    val webUiPort: Int,
    val publicAddress: String)
  extends Serializable {

  Utils.checkHost(host, "Expected hostname")
  assert (port > 0)

  @transient var executors: mutable.HashMap[String, ExecutorDesc] = _ // executorId => info
  @transient var drivers: mutable.HashMap[String, DriverInfo] = _ // driverId => info
  @transient var state: WorkerState.Value = _
  @transient var coresUsed: Int = _
  @transient var memoryUsed: Int = _
  
  /**
   * [SMSpark]: 保存当前的BlockServerWorker节点上的Block信息
   */
  @transient var sblocks: mutable.HashMap[String, SBlockEntry] = _
  //BlockWorker的总可用内存
  def smemoryTotal: Long = (memory * 0.5).toLong

  def smemoryFree: Long = smemoryTotal - smemoryUsed
  //BlockWorker的当前已使用内存
  @transient var smemoryUsed: Long = _

  /**
   * 记录smemory使用量的历史值，第一条记录是开始时间和0L
   */
  @transient var smemoryUsedHistory: mutable.HashMap[Long, Long] = _

  @transient var lastHeartbeat: Long = _

  init()

  def coresFree: Int = cores - coresUsed
  def memoryFree: Int = memory - memoryUsed

  private def readObject(in: java.io.ObjectInputStream): Unit = Utils.tryOrIOException {
    in.defaultReadObject()
    init()
  }

  private def init() {
    executors = new mutable.HashMap
    drivers = new mutable.HashMap
    state = WorkerState.ALIVE
    coresUsed = 0
    memoryUsed = 0
    lastHeartbeat = System.currentTimeMillis()
    
    sblocks = new mutable.HashMap
    smemoryUsed = 0L
    smemoryUsedHistory = new mutable.HashMap
    smemoryUsedHistory += ((lastHeartbeat, smemoryUsed))
  }

  def hostPort: String = {
    assert (port > 0)
    host + ":" + port
  }

  def addExecutor(exec: ExecutorDesc) {
    executors(exec.fullId) = exec
    coresUsed += exec.cores
    memoryUsed += exec.memory
  }

  def removeExecutor(exec: ExecutorDesc) {
    if (executors.contains(exec.fullId)) {
      executors -= exec.fullId
      coresUsed -= exec.cores
      memoryUsed -= exec.memory
    }
  }

  def hasExecutor(app: ApplicationInfo): Boolean = {
    executors.values.exists(_.application == app)
  }

  def addDriver(driver: DriverInfo) {
    drivers(driver.id) = driver
    memoryUsed += driver.desc.mem
    coresUsed += driver.desc.cores
  }

  def removeDriver(driver: DriverInfo) {
    drivers -= driver.id
    memoryUsed -= driver.desc.mem
    coresUsed -= driver.desc.cores
  }

  def webUiAddress : String = {
    "http://" + this.publicAddress + ":" + this.webUiPort
  }

  def setState(state: WorkerState.Value) = {
    this.state = state
  }
  
  /**
   * [SMSpark]: 更新内存使用情况，和BlockInfo
   */
  def updateBlockInfo(newEntry: SBlockEntry) {
    //this.smemoryUsed -= memoryUsed
    
    sblocks.get(newEntry.userDefinedId) match {
      case Some(oldEntry) =>

        updateSMemory(oldEntry.size - newEntry.size)
        sblocks.update(newEntry.userDefinedId, newEntry)
      case None =>
        
    }
      
  }

  private def updateSMemory(delta: Long): Unit = {
    smemoryUsedHistory += ((System.currentTimeMillis(), smemoryUsed))
    smemoryUsed += delta
  }

  def addBlock(blockEntry: SBlockEntry) {
    if (!sblocks.contains(blockEntry.userDefinedId)) {
      updateSMemory(blockEntry.size)
    }
    sblocks += ((blockEntry.userDefinedId, blockEntry))

  }
  
  /**
   * TODO：需要处理是否别的Worker仍然在使用它。****在BlockServerMaster中进行处理
   * 
   */
  def removeBlock(blockUid: String) {
    sblocks.get(blockUid) match {
      case Some(blockEntry) =>
        updateSMemory(-blockEntry.size)
        sblocks.remove(blockUid)
      case None =>
    }
    
  }
  
  /**
   * [SMSpark]: 返回共享存储的内存使用比例
   */
  def smemoryUsePercent: Double = {
    if (smemoryTotal > 0) {
      smemoryUsed.toDouble / smemoryTotal.toDouble
    } else {
      0.00
    }
  }
}

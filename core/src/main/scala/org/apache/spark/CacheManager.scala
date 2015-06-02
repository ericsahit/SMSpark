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

package org.apache.spark

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.rdd.RDD
import org.apache.spark.storage._

/**
 * Spark class responsible for passing RDDs partition contents to the BlockManager and making
 * sure a node doesn't load two copies of an RDD at once.
 */
private[spark] class CacheManager(blockManager: BlockManager) extends Logging {

  /** Keys of RDD partitions that are being computed/loaded. */
  private val loading = new mutable.HashSet[RDDBlockId]

  //当任务执行要获取RDD.iterator()时候，会触发存储机制。
  //读写的流程都从这里入口。
  //1)如果RDD没有被Cache，那么执行rdd.computeOrReadCheckpoint，包装父RDD的迭代器，或者从检查点来读
  //2)如果这个RDD被缓存，那么先到BlockManager中去查找是否寻在：
  //	如果存在，则返回此block的迭代器；
  //	如果不存在，则先计算，然后再缓存。
  //	如果是缓存到内存中，则需要先展开数据，得到数据真实的长度，方便根据内存空间大小进行存储。
  //因为RDD的计算都是惰性的，当进行迭代时，才会执行的层层包装next方法，对每一条记录进行pipeline式的处理。
  //理解RDD的本质其实是构建了一条数据流通道，然后再需要获取结果的时候，从源头开始一条一条构建，中间的窄依赖的RDD都是数据流通道上的一个操作。
  //
  //这里也传入了TaskContext，因为是在执行Task任务内进行存取和判断的
  //
  //1.如果RDD不存在，则先得到此RDD的迭代器computedValues
  //2.调用putInBlockManager方法，将RDD放入到缓存中
  //	1.如果不放入内存，则调用blockManager.putIterator
  //	2.如果放入内存则先unrollSafely展开计算，调用blockManager.putArray进行存储，返回展开后的array
  //
  /** Gets or computes an RDD partition. Used by RDD.iterator() when an RDD is cached. */
  def getOrCompute[T](
      rdd: RDD[T],
      partition: Partition,
      context: TaskContext,
      storageLevel: StorageLevel): Iterator[T] = {

    val key = RDDBlockId(rdd.id, partition.index)
    logDebug(s"Looking for partition $key")
    blockManager.get(key) match {//这里会在本次或者远程来查找，远程查找需要跟BlockManagerMaster通信确定数据位置
      //****为什么会出现Network远程拉去数据，是否没有利用到Block的本地性？
      //answer：在任务调度过程中会利用到Block的本地性，但是任务调度策略还需要仔细研究，为什么会出现那么多的远程拉取数据
      case Some(blockResult) =>
        // Partition is already materialized, so just return its values
        val inputMetrics = blockResult.inputMetrics
        val existingMetrics = context.taskMetrics
          .getInputMetricsForReadMethod(inputMetrics.readMethod) //SparkUI中task的input，memory或network
        existingMetrics.incBytesRead(inputMetrics.bytesRead)//输入的数据量

        val iter = blockResult.data.asInstanceOf[Iterator[T]]
        new InterruptibleIterator[T](context, iter) {
          override def next(): T = {
            existingMetrics.incRecordsRead(1)
            delegate.next()
          }
        }
      case None =>//如果本地或远程都不存在Block，进入写入流程
        // Acquire a lock for loading this partition
        // If another thread already holds the lock, wait for it to finish return its results
        val storedValues = acquireLockForPartition[T](key)//防止其他的线程正在创建此Block
        if (storedValues.isDefined) {
          return new InterruptibleIterator[T](context, storedValues.get)
        }

        // Otherwise, we have to load the partition ourselves
        try {//如果没有被缓存，则先得到迭代器（不一定直接计算），再尝试缓存到内存中
          logInfo(s"Partition $key not found, computing it")
          val computedValues = rdd.computeOrReadCheckpoint(partition, context)

          // If the task is running locally, do not persist the result
          if (context.isRunningLocally) {
            return computedValues
          }

          //放入内存会引起其余一些Block状态变化，例如从内存中替换一些Block，所以要返回updatedBlocks
          // Otherwise, cache the values and keep track of any updates in block statuses
          val updatedBlocks = new ArrayBuffer[(BlockId, BlockStatus)]
          val cachedValues = putInBlockManager(key, computedValues, storageLevel, updatedBlocks)
          val metrics = context.taskMetrics
          val lastUpdatedBlocks = metrics.updatedBlocks.getOrElse(Seq[(BlockId, BlockStatus)]())
          metrics.updatedBlocks = Some(lastUpdatedBlocks ++ updatedBlocks.toSeq)
          new InterruptibleIterator(context, cachedValues)

        } finally {
          loading.synchronized {//计算结束后，释放loading对象，并且通知其他等待的计算线程
            loading.remove(key)
            loading.notifyAll()
          }
        }
    }
  }

  /**
   * Acquire a loading lock for the partition identified by the given block ID.
   *
   * If the lock is free, just acquire it and return None. Otherwise, another thread is already
   * loading the partition, so we wait for it to finish and return the values loaded by the thread.
   */
  private def acquireLockForPartition[T](id: RDDBlockId): Option[Iterator[T]] = {
    loading.synchronized {
      if (!loading.contains(id)) {//加入loading，释放锁，然后开始计算数据
        // If the partition is free, acquire its lock to compute its value
        loading.add(id)
        None
      } else {//
        // Otherwise, wait for another thread to finish and return its result
        logInfo(s"Another thread is loading $id, waiting for it to finish...")
        while (loading.contains(id)) {
          try {
            loading.wait()
          } catch {
            case e: Exception =>
              logWarning(s"Exception while waiting for another thread to load $id", e)
          }
        }
        logInfo(s"Finished waiting for $id")
        val values = blockManager.get(id)
        if (!values.isDefined) {
          /* The block is not guaranteed to exist even after the other thread has finished.
           * For instance, the block could be evicted after it was put, but before our get.
           * In this case, we still need to load the partition ourselves. */
          logInfo(s"Whoever was loading $id failed; we'll try it ourselves")
          loading.add(id)
        }
        values.map(_.data.asInstanceOf[Iterator[T]])
      }
    }
  }

  /**
   * Cache the values of a partition, keeping track of any updates in the storage statuses of
   * other blocks along the way.
   *
   * The effective storage level refers to the level that actually specifies BlockManager put
   * behavior, not the level originally specified by the user. This is mainly for forcing a
   * MEMORY_AND_DISK partition to disk if there is not enough room to unroll the partition,
   * while preserving the the original semantics of the RDD as specified by the application.
   */
  private def putInBlockManager[T](
      key: BlockId,
      values: Iterator[T],
      level: StorageLevel,
      updatedBlocks: ArrayBuffer[(BlockId, BlockStatus)],
      effectiveStorageLevel: Option[StorageLevel] = None): Iterator[T] = {

    val putLevel = effectiveStorageLevel.getOrElse(level)
    if (!putLevel.useMemory) {//如果不需要放入内存，则不需要内存的展开，直接存放
      /*
       * This RDD is not to be cached in memory, so we can just pass the computed values as an
       * iterator directly to the BlockManager rather than first fully unrolling it in memory.
       * 
       * [SMSpark]: 如果使用Tachyon级别，那么存储空间的管理还有效吗？
       */
      updatedBlocks ++=
        blockManager.putIterator(key, values, level, tellMaster = true, effectiveStorageLevel)
      blockManager.get(key) match {
        case Some(v) => v.data.asInstanceOf[Iterator[T]]
        case None =>
          logInfo(s"Failure to store $key")
          throw new BlockException(key, s"Block manager failed to return cached value for $key!")
      }
    } else {
      /*
       * This RDD is to be cached in memory. In this case we cannot pass the computed values
       * to the BlockManager as an iterator and expect to read it back later. This is because
       * we may end up dropping a partition from memory store before getting it back.
       *
       * In addition, we must be careful to not unroll the entire partition in memory at once.
       * Otherwise, we may cause an OOM exception if the JVM does not have enough space for this
       * single partition. Instead, we unroll the values cautiously, potentially aborting and
       * dropping the partition to disk if applicable.
       */
      //
      //展开数据，而且需要逐渐的展开，防止内存爆掉，展开的过程中会进行block的置换（简单的循环block数组），并没有一定的策略
      //参见相关的Jira和PR
      //TODO：使用共享存储时候这里需要修改
      blockManager.memoryStore.unrollSafely(key, values, updatedBlocks) match {
        case Left(arr) =>//可以展开，有足够空间，则存储到内存中
          // We have successfully unrolled the entire partition, so cache it in memory
          updatedBlocks ++=
            blockManager.putArray(key, arr, level, tellMaster = true, effectiveStorageLevel)
          arr.iterator.asInstanceOf[Iterator[T]]//直接返回的展开结果，以供本次使用
        case Right(it) =>//空间不足，看看是否能写入到磁盘
          // There is not enough space to cache this partition in memory
          val returnValues = it.asInstanceOf[Iterator[T]]
          if (putLevel.useDisk) {
            logWarning(s"Persisting partition $key to disk instead.")
            val diskOnlyLevel = StorageLevel(useDisk = true, useMemory = false,
              useOffHeap = false, deserialized = false, putLevel.replication)
            putInBlockManager[T](key, returnValues, level, updatedBlocks, Some(diskOnlyLevel))
          } else {
            returnValues
          }
      }
    }
  }

}

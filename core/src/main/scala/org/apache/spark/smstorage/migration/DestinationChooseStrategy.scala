package org.apache.spark.smstorage.migration

import org.apache.spark.deploy.master.WorkerInfo
import org.apache.spark.smstorage.BlockServerMessages.MigrateDestination
import org.apache.spark.smstorage.{SBlockEntry, SBlockId}

import scala.collection.mutable.ArrayBuffer
import scala.math.Ordering
import scala.util.Random

/**
 * Created by Wang Haihua on 2016/2/5.
 */
abstract class DestinationChooseStrategy {
  def chooseDestination(workers: Seq[WorkerInfo], blockEntry: SBlockEntry): Option[MigrateDestination]
}

class RandomDestinationChooseStrategy(port: Int) extends DestinationChooseStrategy {
  override def chooseDestination(workers: Seq[WorkerInfo], blockEntry: SBlockEntry): Option[MigrateDestination] = {
    val sortedWorkers = workers.sortBy(worker => worker.smemoryUsed.toDouble / worker.smemoryTotal.toDouble)(Ordering[Double])
    val selectedWorker = sortedWorkers.head
    Option(MigrateDestination(selectedWorker.id, selectedWorker.host, port))
    //None
  }


}

case class TargetSatisfication(workerId: String,
                               host: String,
                               var tf: Double,
                               var nrt: Double,
                               var nrs: Double,
                               var mt: Double = 0.0,
                               var satisfication: Double = 0.0)

class LinearWeightDestinationChooseStrategy(port: Int, a2: Double, b2: Double, a3: Double, b3: Double) extends DestinationChooseStrategy {
  override def chooseDestination(workers: Seq[WorkerInfo], entry: SBlockEntry): Option[MigrateDestination] = {

    val arr = new ArrayBuffer[TargetSatisfication](workers.size)

    var rddPartitionCount = 0
    //采用标准化
    var meansMin = 0
    var meansMax = 0
    var stddevMax = 0
    var stddevMin = 0

    workers.foreach { worker =>

      val sameRddPartitionCount = worker.sblocks.keySet.count { globalId =>
        entry.userDefinedId != globalId && SBlockId.isBelongToSameRdd(entry.userDefinedId, globalId)
      }
      rddPartitionCount += sameRddPartitionCount

      val seqSMemory = worker.smemoryUsedHistory.values
      val means = computeMeans(seqSMemory)
      val stddev = computeStddev(seqSMemory, means)

      arr += (TargetSatisfication(worker.id, worker.host, rddPartitionCount, means, stddev))
    }

    val stdTf = standardlize(arr.map(_.tf))
    val stdNrs = standardlize(arr.map(_.nrs))
    val stdNrt = standardlize(arr.map(_.nrt))

    for (i <- 0 to (arr.size - 1)) {
      arr(i).tf = stdTf(i)
      arr(i).nrs = stdNrs(i)
      arr(i).nrt = stdNrt(i)
      arr(i).mt = arr(i).nrs * a2 + arr(i).nrt * b2
      arr(i).satisfication = a3 * arr(i).tf + b3 * arr(i).mt
    }

    val selectedWorker = arr.sortBy(_.satisfication)(Ordering[Double].reverse).head

    Some(MigrateDestination(selectedWorker.workerId, selectedWorker.host, port))

  }

  def computeMeans(xs: Iterable[Long]): Double = xs match {
    case Nil => 0.0
    case ys => ys.reduceLeft(_ + _) / ys.size.toDouble
  }

  def computeStddev(xs: Iterable[Long], avg: Double): Double = xs match {
    case Nil => 0.0
    case ys => math.sqrt((0.0 /: ys) {
      (a,e) => a + math.pow(e - avg, 2.0)
    } / xs.size)
  }

  def standardlize(xs: ArrayBuffer[Double]) = {
    val sp = (xs.max - xs.min)
    xs.map(_ / sp)
  }
}


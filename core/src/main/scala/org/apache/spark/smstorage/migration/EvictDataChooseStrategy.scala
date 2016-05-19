package org.apache.spark.smstorage.migration

import org.apache.spark.SparkConf
import org.apache.spark.smstorage.{SBlockId, SBlockEntry}

import scala.collection.mutable.ArrayBuffer
import scala.collection.{JavaConversions, mutable}
import scala.math.Ordering

/**
 * Created by Wang Haihua on 2016/2/3.
 */
private[smstorage] trait EvictDataChooseStrategy {

  def choose(blockList: mutable.Map[SBlockId, SBlockEntry]): Seq[(SBlockId, SBlockEntry)]

}

private[smstorage] class LRUStrategy extends EvictDataChooseStrategy {
  override def choose(blockList: mutable.Map[SBlockId, SBlockEntry]): Seq[(SBlockId, SBlockEntry)] = {
    blockList.toSeq.sortBy(_._2.lastReadTime)(Ordering[Long].reverse)
  }
}


case class BlockCost(id: SBlockId, cc: Double, vc: Double, af: Double, vf: Double, var cost: Double)

case class GlobalStatistic(var totalApp: Int, var totalVisitCount: Long)

private[smstorage] class LinearWeightingStrategy(conf: SparkConf, statistic: GlobalStatistic) extends EvictDataChooseStrategy {

  val ccWeight = conf.getDouble("", 0.5)
  val vcWeight = 0.3
  val afWeight = 0.2
  val vfWeight = 0.1

  override def choose(blockList: mutable.Map[SBlockId, SBlockEntry]): Seq[(SBlockId, SBlockEntry)] = {
    val arr = ArrayBuffer[BlockCost]()

    var ccSum = 0.0
    var vcSum = 0.0
    var afSum = 0.0
    var vfSum = 0.0

    for ((id, entry) <- blockList) {

      val cc = entry.cons
      val vc = entry.visitLocalCount / entry.visitCount
      val af = entry.usingApps.size / statistic.totalApp
      val vf = entry.visitCount / statistic.totalVisitCount

      ccSum = ccSum + cc
      vcSum = vcSum + vc
      afSum = afSum + af
      vfSum = vfSum + vf
      val blockCost = BlockCost(id, cc, vc, af, vf, 0.0)
      arr += blockCost
    }

    arr.foreach { cost =>
      cost.cost = (cost.cc / ccSum * ccWeight)
        + (cost.vc / vcSum * vcWeight)
        + (cost.af / afSum * afWeight)
        + (cost.vf / vfSum * vfWeight)
    }

    arr.sortBy(_.cost).map(cost => (cost.id, blockList.get(cost.id).get)).toSeq
  }

}

package org.apache.spark.smstorage.migration

import org.apache.spark.smstorage.BlockServerMessages.MigrateDestination
import org.apache.spark.smstorage.{SBlockEntry, SBlockId}

import scala.collection.mutable

/**
 * Created by Wang Haihua on 2016/2/5.
 */
abstract class DestinationChooseStrategy {
  def chooseDestination(block: mutable.Map[SBlockId, SBlockEntry]): Seq[MigrateDestination]
}


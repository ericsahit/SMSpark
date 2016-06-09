package org.apache.spark.smstorage

import org.apache.spark.network.BlockTransferService
import org.apache.spark.smstorage.worker.BlockServerWorkerActor

/**
 * Created by hadoop on 6/8/16.
 */
object SMSparkContext {
  var blockTransferService: BlockTransferService = null
  var bsWorker: BlockServerWorkerActor = null
}

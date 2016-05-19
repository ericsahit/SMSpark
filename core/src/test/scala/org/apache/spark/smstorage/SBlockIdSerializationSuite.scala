package org.apache.spark.smstorage

import java.io.{ByteArrayOutputStream, ObjectOutputStream}

import org.apache.spark.storage.RDDBlockId
import org.scalatest.FunSuite
import org.scalatest.Matchers

/**
 * Created by hadoop on 5/15/15.
 */
class SBlockIdSerializationSuite extends FunSuite with Matchers{

  test("BlockId to SBlockId") {
    val blockId = new RDDBlockId(1, 2)
    val sblockId = SBlockId(blockId)//转换成的SBlock name为空


    assert(sblockId.name.isEmpty())
    assert(sblockId.userDefinedId == "##rdd_1_2")
    assert(sblockId.rddId === "rdd_1_2")

    val sblockIdEqual = new SBlockId("##rdd_1_2")
    assert(sblockId == sblockIdEqual)

    //none datasharing
    val rddId = "spark-application-20160518001518"
    val blockId2 = new RDDBlockId(1, 2, SBlockId.mkGlobalBlockId2(rddId, 1, 2, false))

    val sblockId2 = SBlockId(blockId2)
    assert(sblockId != sblockId2)

    val sblockId3 = new SBlockId("spark-application-20160518001518#1#2")
    assert(sblockId3 == sblockId2)

    //datasharing
    val rddName = "KMeansInput"
    val blockIdDS = new RDDBlockId(1, 2, SBlockId.mkGlobalBlockId2(rddName, 1, 2, true))

    blockIdDS.userDefinedId should be ("KMeansInput##2")

  }

  test("Test block id can or not serializable") {
    val bid = RDDBlockId(1, 2, "kmeans_input")
    //val sbid = SBlockId(bid)
    new ObjectOutputStream(new ByteArrayOutputStream()).writeObject(bid)
  }

  test("Test sblock id can or not serializable") {
    val bid = RDDBlockId(1, 2, "kmeans_input")
    val sbid = SBlockId(bid)
    new ObjectOutputStream(new ByteArrayOutputStream()).writeObject(sbid)
  }

  test("Test local sblock id can or not serializable") {
    val sbid = new TestSBlockId("kmeans", "sss", "dada")
    new ObjectOutputStream(new ByteArrayOutputStream()).writeObject(sbid)
  }
}

//add
case class TestSBlockId(
                val userDefinedId: String,
                val localBlockId: String = "",
                val name: String = "")  {

  /**
   * shared memory entry string，作为唯一的id
   * 每一个共享的BlockId中，共享存储空间都是唯一的
   * entry是否过会很长？
   */
  //def name: String

  /**
   * 本地Block的id
   */
  //def localBlockId: String

  /**
   * RDD的id，作为判断Block是否相同的判断条件
   * TODO: 由输入+每次的变换+下一个Stage的partition+下一个Stage的变换+...组成
   * ****现在以enrtyId为优先判断条件，然后以userDefinedId为判断条件;是否改成以userDefinedId为主要判断条件
   */
  //def rddDepId: Long

  override def toString = if (userDefinedId.isEmpty()) name else userDefinedId
  override def hashCode = if (userDefinedId.isEmpty()) name.hashCode else userDefinedId.hashCode
  override def equals(other: Any): Boolean = other match {
    case o: SBlockId =>
      if (!userDefinedId.isEmpty()) {
        userDefinedId.equals(o.userDefinedId)
      } else {
        name == o.name
      }
  }
}

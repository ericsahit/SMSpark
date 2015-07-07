import breeze.linalg.{Vector, DenseVector, squaredDistance}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

object SparkKMeans {
  
  def parseVector(line: String): Vector[Double] = {
    DenseVector(line.split(' ').map(_.toDouble))
  }
  
  def closestPoint(p: Vector[Double], centers: Array[Vector[Double]]): Int = {
    var closestIndex = 0
    var closest = Double.PositiveInfinity
    
    for (i <- 0 until centers.length) {
      val tempDist = squaredDistance(p, centers(i))
      if (tempDist < closest) {
        closestIndex = i
        closest = tempDist
      }
    }
    closestIndex
  }
  
  def main(args: Array[String]) {
    
    
    val conf = new SparkConf().setAppName("my kmeans")
    val sc = new SparkContext(conf)
    
    //读入文本数据，例如（9.2 9.2 9.2）
    val lines = sc.textFile(args(0))
    //将每行的数据按照空格分开，放到数组里，作为点数据，然后cache
    val points = lines.map(parseVector _).cache
    
    val K = args(1).toInt
    //初始K个中心点
    val kPoints = points.takeSample(withReplacement = false, K)
    var tempDist = 1.0
    
    val iter = args(2).toInt
    //开始每一次迭代
    while (i <- 0 to iter) {
      //对每个点数据，计算所属的分类是哪个，返回(中心点，(数据点, 1))
      val closest = points.map {point =>
        (closestPoint(point, centers), (p, 1))
      }
      //每个聚类的key累加，方便计算新的中心点，这是一个分布式的计算作业
      val pointsStat = closest.reduceByKey { case ((x1, y1), (x2, y2)) => ((x1+x2), (y1+y2)) }
      //计算新的中心点
      val newPoints = pointsStat.map { pair =>
        (pair._1, pair._2._1 * (1.0 / pair._2._2) )
      }.collectAsMap()
      
      tempDist = 0.0 
      for ( j <- 0 until K) {
        tempDist += squaredDistance(kPoints(j), newPoints(j))
        
        kPoints(j) = newPoints(j)
      }
      
      for (newp <- newPoints) {
        kPoints(newp._1) = newp._2
      }
      println("Finished iteration " + k)
    }
    
    println("output: ")
    kPoints.foreach { println(_)}
    
    
    
  }
}
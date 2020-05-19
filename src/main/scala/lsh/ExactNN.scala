package lsh

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

import scala.util.Random

class ExactNN(sqlContext: SQLContext, data: RDD[(String, List[String])], threshold: Double) extends Construction with Serializable {
  def similar(x: (String, List[String]), y: (String, List[String])): Boolean = {
    val xSet = x._2.toSet
    val ySet = y._2.toSet
    val nom: Double = xSet.intersect(ySet).size.toDouble
    val denom: Double = xSet.union(ySet).size.toDouble
    return nom / denom > threshold
  }

  override def eval(rdd: RDD[(String, List[String])]): RDD[(String, Set[String])] = {
    /*
    * This method performs a near-neighbor computation for the data points in rdd against the data points in data.
    * Near-neighbors are defined as the points with a Jaccard similarity that exceeds the threshold
    * data: data points in (movie_name, [keyword_list]) format to compare against
    * rdd: data points in (movie_name, [keyword_list]) format that represent the queries
    * threshold: the similarity threshold that defines near-neighbors
    * return near-neighbors in (movie_name, [nn_movie_names]) as an RDD[(String, Set[String])]
    * */
    rdd
      .cartesian(data)
      .filter(x => similar(x._1, x._2))
      .map(x => (x._1._1, Set[String](x._2._1)))
      .union(rdd.map(x => (x._1, Set[String]())))
      .reduceByKey(_ ++ _)
  }
}

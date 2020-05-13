package lsh

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

class BaseConstruction(sqlContext: SQLContext, data: RDD[(String, List[String])]) extends Construction with Serializable {
  /*
  * Initialize LSH data structures here
  * */
  val univer = {
    val uni = collection.mutable.Map[String, Int]()
    data.foreach(x => {
      x._2.foreach(y => uni.update(y, uni.getOrElse(y, uni.size + 1)))
    })
    uni
  }

  def minHash(keywords: List[String]): Int = {
    keywords
      .map(x => univer.getOrElse(x, Int.MaxValue))
      .min
  }

  val lsh : RDD[(Int, Set[String])] = {
    data
      .map(x => (minHash(x._2), Set(x._1)))
      .reduceByKey(_ ++ _)
  }

  override def eval(rdd: RDD[(String, List[String])]): RDD[(String, Set[String])] = {
    /*
    * This method performs a near-neighbor computation for the data points in rdd against the data points in data.
    * You need to perform the queries by using LSH with min-hash.
    * The perturbations needs to be consistent - decided once and randomly for each BaseConstructor object
    * sqlContext: current SQLContext
    * data: data points in (movie_name, [keyword_list]) format to compare against
    * rdd: data points in (movie_name, [keyword_list]) format that represent the queries
    * return near-neighbors in (movie_name, [nn_movie_names]) as an RDD[(String, Set[String])]
    * */
    rdd
      .map(x => (x._1, lsh.filter(y => y._1 == minHash(x._2)).map(z => z._2).reduce(_ ++ _)))
  }
}

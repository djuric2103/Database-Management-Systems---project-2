package lsh

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

class BaseConstruction(sqlContext: SQLContext, data: RDD[(String, List[String])]) extends Construction with Serializable {
  /*
  * Initialize LSH data structures here
  * */

  //val a = new util.Random().nextInt()
  val a = 3
  def minHash(keywords: List[String]): Int = {
    keywords.map(x => x.hashCode ^ a).min
  }

  val lsh : RDD[(Int, Set[String])] = data.map(x => (minHash(x._2), Set(x._1))).reduceByKey(_ ++ _)
  /*{
    val d = data.map(x => (minHash(x._2), Set(x._1))).reduceByKey(_ ++ _)
    d.foreach(x => println(x))

    d
  }*/


  def getSimilar(xHash: Int) : Set[String] = {
    println(xHash)
    val sim : RDD[(Int, Set[String])] = lsh.filter(x => {x._1 == xHash})
    if(sim.count() > 0) return sim.first()._2
    return Set[String]()
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
    println(getSimilar(-1579453983))
    println(getSimilar(-1205604449))

    rdd.map(x => (x._1, getSimilar(minHash(x._2))))
  }
}
package lsh

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

class BaseConstructionBroadcast(sqlContext: SQLContext, data: RDD[(String, List[String])]) extends Construction with Serializable {
  /*
  * Initialize LSH data structures here
  * You need to broadcast the data structures to all executors and use them locally
  * */
  val a = new util.Random().nextInt()

  def minHash(keywords: List[String]): Int = {
    keywords.map(x => x.hashCode ^ a).min
  }
  val l = data.map(x => (minHash(x._2), List(x._1))).reduceByKey(_ ++ _).map(x => (x._1, x._2.toSet))
  val data_df = sqlContext.createDataFrame(l)
  val lsh_broadcasted = sqlContext.sparkContext.broadcast(data_df, data_df.schema)

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
   /* lsh_broadcasted.value.
    rdd.flatMap((x_key, x_val) => lsh_broadcasted.value.get.(minHash(x_val)).map{
        y_val => (x_key, y_val)
      }
    )
    rdd
      .map(x => (minHash(x._2),x))
      .join(lsh_broadcasted.value.get(_))
      .map(x => (x._2._1._1, x._2._2))
      .union(rdd.map(x => (x._1, Set[String]())))
      .reduceByKey(_++_)*/
    ???
  }
}

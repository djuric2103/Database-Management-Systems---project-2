package lsh

import java.io.File

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}


object Main {
  def recall(ground_truth: RDD[(String, Set[String])], lsh_truth: RDD[(String, Set[String])]): Double = {
    /*
    * Compute the recall for each near-neighbor LSH query against the accurate result
    * Then, compute the average across all queries
    * ground_truth: results of queries in (movie_name, [nn_movie_names]) format produced by ExactNN
    * lsh_truth: results of queries in (movie_name, [nn_movie_names]) format produced by an LSH Construction
    * returns average recall
    * */
    val r = ground_truth
      .join(lsh_truth)
      .filter(x => x._2._1.size > 0)
      .map(x => x._2._1.intersect(x._2._2).size.asInstanceOf[Double] / x._2._1.size.asInstanceOf[Double])
      .mean()
    println(r)

    return r
  }

  def precision(ground_truth: RDD[(String, Set[String])], lsh_truth: RDD[(String, Set[String])]): Double = {
    /*
    * Compute the precision for each near-neighbor LSH query against the accurate result
    * Then, compute the average across all queries
    * ground_truth: results of queries in (movie_name, [nn_movie_names]) format produced by ExactNN
    * lsh_truth: results of queries in (movie_name, [nn_movie_names]) format produced by an LSH Construction
    * returns average precision
    * */
    val r = ground_truth
      .join(lsh_truth)
      .filter(x => x._2._2.size > 0)
      .map(x => x._2._1.intersect(x._2._2).size.asInstanceOf[Double] / x._2._2.size.asInstanceOf[Double])
      .mean()
    println(r)
    return r
  }

  def query1(sc: SparkContext, sqlContext: SQLContext): Unit = {
    val corpus_file = new File(getClass.getResource("/lsh-corpus-small.csv").getFile).getPath

    val rdd_corpus = sc
      .textFile(corpus_file)
      .map(x => x.toString.split('|'))
      .map(x => (x(0), x.slice(1, x.size).toList))

    val query_file = new File(getClass.getResource("/lsh-query-1.csv").getFile).getPath

    val rdd_query = sc
      .textFile(query_file)
      .map(x => x.toString.split('|'))
      .map(x => (x(0), x.slice(1, x.size).toList))

    val exact: Construction = new ExactNN(sqlContext, rdd_corpus, 0.3)
    val lsh: Construction = new ANDConstruction((for(i <- 0 to 4) yield new BaseConstructionBroadcast(sqlContext, rdd_corpus)).toList)

    val ground = exact.eval(rdd_query)
    val res = lsh.eval(rdd_query)

    assert(recall(ground, res) > 0.7)
    assert(precision(ground, res) > 0.98)
  }

  def query2(sc: SparkContext, sqlContext: SQLContext): Unit = {
    val corpus_file = new File(getClass.getResource("/lsh-corpus-small.csv").getFile).getPath

    val rdd_corpus = sc
      .textFile(corpus_file)
      .map(x => x.toString.split('|'))
      .map(x => (x(0), x.slice(1, x.size).toList))

    val query_file = new File(getClass.getResource("/lsh-query-2.csv").getFile).getPath

    val rdd_query = sc
      .textFile(query_file)
      .map(x => x.toString.split('|'))
      .map(x => (x(0), x.slice(1, x.size).toList))

    val exact: Construction = new ExactNN(sqlContext, rdd_corpus, 0.3)

    val lsh: Construction = new ORConstruction((for(i <- 0 until 3) yield new BaseConstructionBroadcast(sqlContext, rdd_corpus)).toList)

    val ground = exact.eval(rdd_query)
    val res = lsh.eval(rdd_query)


    assert(recall(ground, res) > 0.9)
    assert(precision(ground, res) > 0.45)
  }

  def query0(sc: SparkContext, sqlContext: SQLContext): Unit = {
    //val corpus_file = new File(getClass.getResource("/lsh-corpus-small.csv").getFile).getPath
    val corpus_file = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", "|")
      .load("/user/cs422/lineorder_small.tbl")

    val rdd_corpus = sc
      .textFile(corpus_file)
      .map(x => x.toString.split('|'))
      .map(x => (x(0), x.slice(1, x.size).toList))

    val query_file = new File(getClass.getResource("/lsh-query-0.csv").getFile).getPath

    val rdd_query = sc
      .textFile(query_file)
      .map(x => x.toString.split('|'))
      .map(x => (x(0), x.slice(1, x.size).toList))

    val exact: Construction = new ExactNN(sqlContext, rdd_corpus, 0.3)
    val lsh: Construction = new BaseConstructionBroadcast(sqlContext, rdd_corpus)

    val ground = exact.eval(rdd_query)
    val res = lsh.eval(rdd_query)

    assert(recall(ground, res) > 0.83)
    assert(precision(ground, res) > 0.70)
  }


  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("app")//.setMaster("local[*]")
    val sc = SparkContext.getOrCreate(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    query0(sc, sqlContext)
    query1(sc, sqlContext)
    query2(sc, sqlContext)
  }
}

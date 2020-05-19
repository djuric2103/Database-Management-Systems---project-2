package lsh

import java.io.File

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
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
      .filter(x => x._2.size > 0)
      .join(lsh_truth)
      .map(x => x._2._1.intersect(x._2._2).size.asInstanceOf[Double] / x._2._1.size.asInstanceOf[Double])
      .mean()
    println("Recall"+r)

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
      .join(lsh_truth.filter(x => x._2.size > 0))
      .map(x => x._2._1.intersect(x._2._2).size.asInstanceOf[Double] / x._2._2.size.asInstanceOf[Double])
      .mean()
    println("Precision"+r)
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
    val lsh: Construction = new ANDConstruction((for(i <- 0 to 4) yield new BaseConstruction(sqlContext, rdd_corpus)).toList)

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

    val lsh: Construction = new ORConstruction((for(i <- 0 until 3) yield new BaseConstruction(sqlContext, rdd_corpus)).toList)

    val ground = exact.eval(rdd_query)
    val res = lsh.eval(rdd_query)


    assert(recall(ground, res) > 0.9)
    assert(precision(ground, res) > 0.45)
  }

  def query0(sc: SparkContext, sqlContext: SQLContext): Unit = {
    val corpus_file = new File(getClass.getResource("/lsh-corpus-small.csv").getFile).getPath

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
    val lsh: Construction = new BaseConstruction(sqlContext, rdd_corpus)


    val ground = exact.eval(rdd_query)
    val res = lsh.eval(rdd_query)
    assert(recall(ground, res) > 0.83)
    assert(precision(ground, res) > 0.70)
//    res.saveAsTextFile("RDDSaved")
  }

  def loadRDD(sqlContext: SQLContext, file: String): RDD[Row] = {
    sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "false")
      .option("inferSchema", "true")
      .option("delimiter", ",")
      .load(file).rdd
  }

  def query_cluster(c : String, q : String, sqlContext : SQLContext, b : Boolean): Unit ={
    val rdd_corpus = loadRDD(sqlContext, "/user/cs422/lsh-corpus-"+c+".csv").map(x => x.toString.split('|')).map(x => (x(0), x.slice(1, x.size).toList))
    val rdd_querry = loadRDD(sqlContext, "/user/cs422/lsh-query-"+q+".csv").map(x => x.toString.split('|')).map(x => (x(0), x.slice(1, x.size).toList))
    //println(rdd_corpus.count() + " " + rdd_querry.count())
    //val exact : Construction = new ExactNN(sqlContext, rdd_corpus, 0.3)
    val a = "EXACT_Q_"+q+"_FINISH"
    val ground : RDD[(String, Set[String])] = sqlContext.sparkContext.textFile("../group-48/EXACT_Q_"+q+"_FINISH").map(x => (x.substring(1, x.indexOf(",Set(")),x.substring(x.indexOf(",Set(") + 5, x.length - 2).split(", ").toSet))
    val base: Construction = {
      new ANDConstruction(
        (for(i <- 0 until 2)
          yield new ORConstruction(
            (for(j <- 0 until 2)
              yield {
                if(b) new BaseConstructionBroadcast(sqlContext, rdd_corpus)
                else new BaseConstruction(sqlContext, rdd_corpus)
              }).toList)).toList)
    }
    //val baseBroadcast : Construction = new BaseConstructionBroadcast(sqlContext, rdd_corpus)
    //val date : String= DateTimeFormatter.ofPattern("dd HH mm").format(LocalDateTime.now)
    //val ground = exact.eval(rdd_querry)
    val lsh = base.eval(rdd_querry)
    recall(ground, lsh)
    precision(ground, lsh)
    //ground.saveAsTextFile("/user/group-48/EXACT_Q_"+q+"_FINISH")
    //lsh.saveAsTextFile("/user/group-48/lsh "+date)

    //val lshBroadcast = baseBroadcast.eval(rdd_querry)
    //lshBroadcast.saveAsTextFile("/user/group-48/lshBroadcast")
  }

  def queryLocal(sc: SparkContext, sqlContext: SQLContext, c: String, q: String): Unit = {
    val corpus_file = new File(getClass.getResource("/lsh-corpus-"+c+".csv").getFile).getPath

    val rdd_corpus = sc
      .textFile(corpus_file)
      .map(x => x.toString.split('|'))
      .map(x => (x(0), x.slice(1, x.size).toList))

    val query_file = new File(getClass.getResource("/lsh-query-"+q+".csv").getFile).getPath

    val rdd_query = sc
      .textFile(query_file)
      .map(x => x.toString.split('|'))
      .map(x => (x(0), x.slice(1, x.size).toList))

    val exact: Construction = new ExactNN(sqlContext, rdd_corpus, 0.3)
    //val lsh: Construction = new BaseConstruction(sqlContext, rdd_corpus)


    val ground = exact.eval(rdd_query)
    ground.saveAsTextFile("ExactQ"+q)
    //val res = lsh.eval(rdd_query)
    //assert(recall(ground, res) > 0.83)
    //assert(precision(ground, res) > 0.70)
    //    res.saveAsTextFile("RDDSaved")
  }
  def queryLocalTest(sc: SparkContext, sqlContext: SQLContext, c: String, q: String, b : Boolean): Unit = {
    val corpus_file = new File(getClass.getResource("/lsh-corpus-"+c+".csv").getFile).getPath

    val rdd_corpus = sc
      .textFile(corpus_file)
      .map(x => x.toString.split('|'))
      .map(x => (x(0), x.slice(1, x.size).toList))

    val query_file = new File(getClass.getResource("/lsh-query-"+q+".csv").getFile).getPath

    val rdd_query = sc
      .textFile(query_file)
      .map(x => x.toString.split('|'))
      .map(x => (x(0), x.slice(1, x.size).toList))

    val ground : RDD[(String, Set[String])] = sc.textFile("ExactQ"+q).map(x => (x.substring(1, x.indexOf(",Set(")),x.substring(x.indexOf(",Set(") + 5, x.length - 2).split(", ").toSet))
    //ground.foreach(x => println(x))
    val lsh: Construction = {
      new ANDConstruction(
        (for(i <- 0 until 4)
          yield new ORConstruction(
            (for(j <- 0 until 4)
              yield {
                if(b) new BaseConstructionBroadcast(sqlContext, rdd_corpus)
                else new BaseConstruction(sqlContext, rdd_corpus)
              }).toList)).toList)
    }
    val res = lsh.eval(rdd_query)
    recall(ground, res)
    precision(ground, res)
//    res.saveAsTextFile("LSH "+ (if(b) "Broadcast" else "Base") + q)
    //    res.saveAsTextFile("RDDSaved")
  }


  /*def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("app").setMaster("local[*]")
    val sc = SparkContext.getOrCreate(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    query0(sc, sqlContext)
    query1(sc, sqlContext)
    query2(sc, sqlContext)
  }*/


  def query_cluster_FILE(c : String, q : String, sqlContext : SQLContext): Unit ={
    val rdd_corpus = loadRDD(sqlContext, "/user/cs422/lsh-corpus-"+c+".csv").map(x => x.toString.split('|')).map(x => (x(0), x.slice(1, x.size).toList))
    val rdd_querry = loadRDD(sqlContext, "/user/cs422/lsh-query-"+q+".csv").map(x => x.toString.split('|')).map(x => (x(0), x.slice(1, x.size).toList))
    //println(rdd_corpus.count() + " " + rdd_querry.count())
    val exact : Construction = new ExactNN(sqlContext, rdd_corpus, 0.3)
    val ground = exact.eval(rdd_querry)
    ground.saveAsTextFile("/user/group-48/EXACT_Q_"+q+"_FINISH")
  }

  def query_cluster_FILE_lsh(c : String, q : String, sqlContext : SQLContext, b : Boolean): Unit ={
    val rdd_corpus = loadRDD(sqlContext, "/user/cs422/lsh-corpus-"+c+".csv").map(x => x.toString.split('|')).map(x => (x(0), x.slice(1, x.size).toList))
    val rdd_querry = loadRDD(sqlContext, "/user/cs422/lsh-query-"+q+".csv").map(x => x.toString.split('|')).map(x => (x(0), x.slice(1, x.size).toList))
    //println(rdd_corpus.count() + " " + rdd_querry.count())
    val base: Construction =  if(b) new BaseConstructionBroadcast(sqlContext, rdd_corpus) else new BaseConstruction(sqlContext, rdd_corpus)
    val lsh = base.eval(rdd_querry)
    lsh.saveAsTextFile("/user/group-48/EXACT_Q_"+q+"_FINISHED11_"+ (if(b)"BROADCAST1" else ""))
  }

 def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("Project2-group-48")
      .getOrCreate()
    val sqlContext = spark.sqlContext
   //query_cluster("medium", "3", sqlContext, true)
   //query_cluster("medium", "4", sqlContext,true)
   //query_cluster("medium", "5", sqlContext, true)
   //query_cluster_FILE("small", "0", sqlContext)
   //query_cluster_FILE("small", "1", sqlContext)
   //query_cluster_FILE("small", "2", sqlContext)
   //query_cluster("small", "2", sqlContext, true)
   query_cluster_FILE_lsh("large","7",sqlContext, true)
 }
}

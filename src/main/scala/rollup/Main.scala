package rollup

import org.apache.spark.sql.functions._
import java.io._

import org.apache.spark.sql.{Row, SQLContext, SparkSession}

object Main {
  System.setProperty("hadoop.home.dir", "C:\\Program Files\\Hadoop");

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("Project2")
      .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
      
    // val rdd_lineorder_small = spark.sqlContext.read
    // .format("com.databricks.spark.csv")
    // .option("header", "true")
    // .option("inferSchema", "true")
    // .option("delimiter", "|")
    // .load("/user/group-48/test/lineorder_small.tbl")

    // val rdd = rdd_lineorder_small.rdd

    // var groupingList = List(0, 1, 3)
    // val rollup = new RollupOperator

    // //val res = rollup.rollup_naive(rdd, groupingList, 8, "COUNT")
    // val res = rollup.rollup(rdd, groupingList, 8, "COUNT")
    // res.saveAsTextFile("/user/group-48/result")

    // val res2 = rollup.rollup_naive(rdd, groupingList, 8, "COUNT")
    // res2.saveAsTextFile("/user/group-48/resultNaive")

    // // use the following code to evaluate the correctness of your results
    // val correctRes = rdd_lineorder_small.rollup("lo_orderkey", "lo_linenumber", "lo_partkey").agg(COUNT("lo_quantity")).rdd
    //                            .map(row => (row.toSeq.toList.dropRight(1).filter(x => x != null), row(row.size - 1)))
    // correctRes.saveAsTextFile("/user/group-48/resultCorrect")           
    
    

    val rdd_lineorder_small = spark.sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("delimiter", "|")
    .load("/user/cs422/lineorder_small.tbl")

    val rdd_lineorder_medium = spark.sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("delimiter", "|")
    .load("/user/cs422/lineorder_medium.tbl")

    val rdd_lineorder_big = spark.sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("delimiter", "|")
    .load("/user/cs422/lineorder_big.tbl")

    val rdd = rdd_lineorder_small.rdd

    var groupingList = List(0, 1, 2, 4, 5, 6, 7, 8, 9)
    val rollup = new RollupOperator

    val i = (4 : Int).toString()

    for(i <- 0 to 10){
      for ((rdd, str) <- List(rdd_lineorder_small.rdd, rdd_lineorder_medium.rdd, rdd_lineorder_big.rdd).zip(List("small","medium", "big"))){
        for (end <- 2 until 9){
          val aux_groupingList = groupingList.slice(0, end)
          if (str == "big" && aux_groupingList.length > 5) {
            var s = System.nanoTime
            val res = rollup.rollup(rdd, aux_groupingList, 3, "COUNT")
            println(s"O,count,${str},${aux_groupingList.length},${(System.nanoTime-s)/1e9}")
            // res.saveAsTextFile("/user/group-48/result" ++ i)

            s = System.nanoTime
            val res2 = rollup.rollup_naive(rdd, aux_groupingList, 3, "COUNT")
            println(s"N,count,${str},${aux_groupingList.length},${(System.nanoTime-s)/1e9}")

            s = System.nanoTime
            val res_1 = rollup.rollup(rdd, aux_groupingList, 3, "COUNT").collect
            println(s"O,collect,${str},${aux_groupingList.length},${(System.nanoTime-s)/1e9}")
            // res.saveAsTextFile("/user/group-48/result" ++ i)

            s = System.nanoTime
            val res2_1 = rollup.rollup_naive(rdd, aux_groupingList, 3, "COUNT").collect
            println(s"N,collect,${str},${aux_groupingList.length},${(System.nanoTime-s)/1e9}")
          }
        }
      }
  }

    // var s = System.nanoTime
    // val res = rollup.rollup(rdd, groupingList, 3, "COUNT")
    // println("Naive: "+(System.nanoTime-s)/1e9+"s")
    // // res.saveAsTextFile("/user/group-48/result" ++ i)

    // var s = System.nanoTime
    // val res2 = rollup.rollup_naive(rdd, groupingList, 3, "COUNT")
    // println("Naive: "+(System.nanoTime-s)/1e9+"s")


    // res2.saveAsTextFile("/user/group-48/resultNaive" ++ i)

    // use the following code to evaluate the correctness of your results
    // val correctRes = rdd_lineorder_small.rollup("a", "b", "c").agg(COUNT("d")).rdd
    //                            .map(row => (row.toSeq.toList.dropRight(1).filter(x => x != null), row(row.size - 1)))
    // correctRes.saveAsTextFile("/user/group-48/resultCorrect" ++ i)           
  }
}
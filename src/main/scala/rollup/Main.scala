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

    // //val res = rollup.rollup_naive(rdd, groupingList, 8, "SUM")
    // val res = rollup.rollup(rdd, groupingList, 8, "SUM")
    // res.saveAsTextFile("/user/group-48/result")

    // val res2 = rollup.rollup_naive(rdd, groupingList, 8, "SUM")
    // res2.saveAsTextFile("/user/group-48/resultNaive")

    // // use the following code to evaluate the correctness of your results
    // val correctRes = rdd_lineorder_small.rollup("lo_orderkey", "lo_linenumber", "lo_partkey").agg(sum("lo_quantity")).rdd
    //                            .map(row => (row.toSeq.toList.dropRight(1).filter(x => x != null), row(row.size - 1)))
    // correctRes.saveAsTextFile("/user/group-48/resultCorrect")           
    
    

    val rdd_lineorder_small = spark.sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("delimiter", "|")
    .load("/user/group-48/test/example.tbl")

    val rdd = rdd_lineorder_small.rdd

    var groupingList = List(0, 1, 2)
    val rollup = new RollupOperator

    val i = (115 : Int).toString()
    // val res = rollup.rollup(rdd, groupingList, 3, "SUM")
    // res.saveAsTextFile("/user/group-48/result" ++ i)

    val res2 = rollup.rollup_naive(rdd, groupingList, 3, "SUM")
    res2.saveAsTextFile("/user/group-48/resultNaive" ++ i)

    // use the following code to evaluate the correctness of your results
    // val correctRes = rdd_lineorder_small.rollup("a", "b", "c").agg(sum("d")).rdd
    //                            .map(row => (row.toSeq.toList.dropRight(1).filter(x => x != null), row(row.size - 1)))
    // correctRes.saveAsTextFile("/user/group-48/resultCorrect" ++ i)           
  }
}
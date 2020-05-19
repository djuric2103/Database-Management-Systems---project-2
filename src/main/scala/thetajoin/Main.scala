package thetajoin

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{SQLContext, Row}


import java.io._

object Main {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("Project2")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val attrIndex1 = 2
    val attrIndex2 = 2

    val rdd1 = loadRDD(spark.sqlContext, "/user/cs422/taxA4K.csv")

    // val rdd_1 = spark.sqlContext.read
    // .format("com.databricks.spark.csv")
    // // .option("header", "true")
    // // .option("inferSchema", "true")
    // // .option("delimiter", "|")
    // .load("/user/group-48/test/dat1_4.csv")
    // val rdd1 = rdd_1.rdd

    val rdd2 = loadRDD(spark.sqlContext, "/user/cs422/taxB4K.csv")
    
    // val rdd_2 = spark.sqlContext.read
    // .format("com.databricks.spark.csv")
    // // .option("header", "true")
    // // .option("inferSchema", "true")
    // // .option("delimiter", "|")
    // .load("/user/group-48/test/dat2_4.csv")
    // val rdd2 = rdd_2.rdd
    for (i <- 5 to 10){
    // val thetaJoin = new ThetaJoin(4)
    
    var s : Double = System.nanoTime

    for (part <- List(2,4,6,8,10,12,14,16,20)){ 
      val thetaJoin = new ThetaJoin(part)
      s = System.nanoTime
      thetaJoin.ineq_join(rdd1, rdd2, attrIndex1, attrIndex2, "<")
      println(s"$part, ${(System.nanoTime-s)/1e9}")
    }
    // res.foreach(x => println(x))


    /*val rdd1 = loadRDD(spark.sqlContext, "/dat1_6.csv")
    val rdd2 = loadRDD(spark.sqlContext, "/dat2_9.csv")

    val thetaJoin = new ThetaJoin(6)
    val res = thetaJoin.ineq_join(rdd1, rdd2, attrIndex1, attrIndex2, "<")
    println(res.count)*/


    // use the cartesian product to verify correctness of your result
      s = System.nanoTime
      val cartesianRes = rdd1.cartesian(rdd2)
                          .filter(x => x._1(attrIndex1).asInstanceOf[Int] < x._2(attrIndex2).asInstanceOf[Int])
                          .map(x => (x._1(attrIndex1).asInstanceOf[Int], x._2(attrIndex2).asInstanceOf[Int]))
      cartesianRes.count
      println(s"${0}, ${(System.nanoTime-s)/1e9}")
    }
    //cartesianRes.foreach(x => println(x))

    // val gotten = res.sortBy(x => (x._1, x._2)).collect().toList
    // val correct = cartesianRes.sortBy(x => (x._1, x._2)).collect().toList
    // println("Gotten:")
    // gotten.foreach(println)
    // println(gotten.zip(correct))
    // println("Correct:")
    // correct.foreach(println)
    // println((gotten.length, correct.length))
    // assert(gotten.equals(correct))
    // assert(res.sortBy(x => (x._1, x._2)).collect().toList.equals(cartesianRes.sortBy(x => (x._1, x._2)).collect().toList) )
  }

  def loadRDD(sqlContext: SQLContext, file: String): RDD[Row] = {
    sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "false")
      .option("inferSchema", "true")
      .option("delimiter", ",")
      .load(file).rdd
  }
}

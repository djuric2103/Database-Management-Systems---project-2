package rollup

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import scala.math._
import scala.{Double}

class RollupOperator() {
// NONE OF THESE FUNCTIONS RETURN TUPLES WIT "ALL" VALUES, TUPLES THAT SHOULD BE (key1, key2, ALL, ALL, ALL), ARE (key1, key2)

  // Performs a group by certain keys
  def groupBy(dataset: RDD[Row], grpIndex: List[Int], aggIndex: Int, agg: String): RDD[(List[Any], Double)] = {
    agg match {
      case "COUNT" => 
        val tupleSplit = (t : Row) => {
          if (t.isNullAt(aggIndex)) 
            (grpIndex.map(i => t(i)), 0.0)
          else 
            (grpIndex.map(i => t(i)), 1.0)
          }
        val seqOp = (accumulator: Double, element: Double) =>  accumulator + element
        val combOp = (x: Double, y: Double) =>  x + y
        val zeroVal = 0.0
        dataset.map(tupleSplit).aggregateByKey(zeroVal)(seqOp, combOp) 
      case "SUM" => 
        def tupleSplit = (t : Row) => (grpIndex.map(i => t(i)),  t(aggIndex).toString().toDouble)
        def seqOp = (accumulator: Double, element: Double) =>  accumulator + element
        def combOp = (x: Double, y: Double) => x + y
        val zeroVal = 0.0
        dataset.map(tupleSplit).aggregateByKey(zeroVal)(seqOp, combOp) 
      case "MIN" => 
        def tupleSplit = (t : Row) => (grpIndex.map(i => t(i)),  t(aggIndex).toString().toDouble)
        def seqOp = (accumulator: Double, element: (Double)) =>  min(accumulator, element)
        def combOp = (x: Double, y: Double) =>  min(x, y)
        val zeroVal = Double.MaxValue
        dataset.map(tupleSplit).aggregateByKey(zeroVal)(seqOp, combOp) 
      case "MAX" => 
        def tupleSplit =  (t : Row) => (grpIndex.map(i => t(i)), if (t.isNullAt(aggIndex)) 0.0 else 1.0)
        def seqOp =(accumulator: Double, element: (Double)) =>  max(accumulator, element)
        def combOp = (x: Double, y: Double) =>  max(x,y)
        val zeroVal = Double.MinValue
        dataset.map(tupleSplit).aggregateByKey(zeroVal)(seqOp, combOp) 
      case "AVG" => 
        def tupleSplit = (t : Row) => (grpIndex.map(i => t(i)), (if (t.isNullAt(aggIndex)) (0.0, 0.0) else (t(aggIndex).toString().toDouble, 1.0)))
        def seqOp = (acc: (Double, Double), x: (Double, Double)) =>  (acc._1 + x._1, acc._2 + x._2)
        def combOp = (x: (Double, Double), y: (Double, Double)) =>   (x._1   + y._1, x._2  + y._2)
        val zeroVal = (0.0, 0.0)
        dataset.map(tupleSplit).aggregateByKey(zeroVal)(seqOp, combOp).map(v => (v._1, v._2._1 / v._2._2))
    }
  }

  def groupByAVG(dataset: RDD[Row], grpIndex: List[Int], aggIndex: Int): RDD[(List[Any], (Double, Double))] = {
    def tupleSplit = (t : Row) => (grpIndex.map(i => t(i)), (if (t.isNullAt(aggIndex)) (0.0, 0.0) else (t(aggIndex).toString().toDouble, 1.0)))
    def seqOp = (acc: (Double, Double), x: (Double, Double)) =>  (acc._1 + x._1, acc._2 + x._2)
    def combOp = (x: (Double, Double), y: (Double, Double)) =>   (x._1   + y._1, x._2  + y._2)
    val zeroVal = (0.0, 0.0)
    dataset.map(tupleSplit).aggregateByKey(zeroVal)(seqOp, combOp)
  }

  // Do the next roll up, group the groups according to the next key.
  def rollUpNextAvg(dataset: RDD[(List[Any], (Double, Double))]): RDD[(List[Any], (Double, Double))] = {
    val tupleSplit = (t : (List[Any], (Double, Double))) => (t._1.reverse.tail.reverse, t._2)
    val seqOp  = (acc: (Double, Double), x: (Double, Double)) =>  (acc._1 + x._1, acc._2 + x._2)
    val combOp = (x: (Double, Double), y: (Double, Double)) =>   (x._1   + y._1, x._2  + y._2)
    val zeroVal = (0.0, 0.0)
    dataset.map(tupleSplit).aggregateByKey(zeroVal)(seqOp, combOp) 
  }

  def rollUpNext(dataset: RDD[(List[Any], Double)], agg: String): RDD[(List[Any], Double)] = {
    val normal = (t : (List[Any], Double)) => (t._1.reverse.tail.reverse, t._2)
    (agg match {
      case "COUNT" => 
        val seqOp = (accumulator: Double, element: Double) =>  accumulator + element
        val combOp = (x: Double, y: Double) =>  x + y
        val zeroVal = 0.0
        dataset.map(normal).aggregateByKey(zeroVal)(seqOp, combOp) 
      case "SUM" => 
        val seqOp = (accumulator: Double, element: Double) =>  accumulator + element
        val combOp = (x: Double, y: Double) =>  x + y
        val zeroVal = 0.0
        dataset.map(normal).aggregateByKey(zeroVal)(seqOp, combOp) 
      case "MIN" => 
        val seqOp = (accumulator: Double, element: (Double)) =>  min(accumulator, element)
        val combOp = (x: Double, y: Double) =>  min(x, y)
        val zeroVal = Double.MaxValue
        dataset.map(normal).aggregateByKey(zeroVal)(seqOp, combOp) 
      case "MAX" => 
        val seqOp =(accumulator: Double, element: (Double)) =>  max(accumulator, element)
        val combOp = (x: Double, y: Double) =>  max(x,y)
        val zeroVal = Double.MinValue
        dataset.map(normal).aggregateByKey(zeroVal)(seqOp, combOp) 
    })//.cache
  }

    /*
  * This method gets as input one dataset, the indexes of the grouping attributes of the rollup (ROLLUP clause)
  * the index of the attribute on which the aggregation is performed
  * and the aggregate function (it has to be one of "COUNT", "SUM", "MIN", "MAX", "AVG")
  * and returns an RDD with the result in the form of <key = List[Any], value = Double> pairs.
  * The key is used to uniquely identify a group that corresponds to a certain combination of attribute values.
  * You are free to do that following your own naming convention.
  * The value is the aggregation result.
  * You are not allowed to change the definition of this function or the names of the aggregate functions.
  * */
  def rollup(dataset: RDD[Row], groupingAttributeIndexes: List[Int], aggAttributeIndex: Int, agg: String): RDD[(List[Any], Double)] = {
    val aggIndex = aggAttributeIndex

    var idx = 0

    agg match {
      case "AVG" =>
        val first_rollup = groupByAVG(dataset, groupingAttributeIndexes, aggIndex)//.cache
        var rollups = List(first_rollup)
        
        while (idx < groupingAttributeIndexes.length) {
          val next_rollup = rollUpNextAvg(rollups.head)
          rollups = List(next_rollup) ++ rollups
          idx += 1
        }
        rollups.reduce(_ ++ _).map(v => (v._1, v._2._1 / v._2._2))
      case _ => 
        val first_rollup = groupBy(dataset, groupingAttributeIndexes, aggIndex,  agg)//.cache
        var rollups = List(first_rollup)

        while (idx < groupingAttributeIndexes.length) {
          val next_rollup = rollUpNext(rollups.head, agg)
          rollups = List(next_rollup) ++ rollups
          idx += 1
        }
        rollups.reduce(_ ++ _).persist
    }
    // No code repetition multiple castings
    // val first_rollup = if (agg == "AVG") groupByAVG(dataset, groupingAttributeIndexes, aggIndex) else groupBy(dataset, groupingAttributeIndexes, aggIndex,  agg)
    // var rollups : List[RDD[(List[Any], Any)]] = List(first_rollup.asInstanceOf[RDD[(List[Any], Any)]])
    // var idx = 0
    // while (idx < groupingAttributeIndexes.length) {
    //   val next_rollup = if (agg == "AVG")  rollUpNextAvg(rollups.head.asInstanceOf[RDD[(List[Any], (Double, Double))]]) else rollUpNext(rollups.head.asInstanceOf[RDD[(List[Any], Double)]], agg)
    //   rollups = List(next_rollup.asInstanceOf[RDD[(List[Any], Any)]]) ++ rollups
    //   idx += 1
    // }
    // val union = rollups.reduce(_ ++ _)
    // if (agg == "AVG") union.asInstanceOf[RDD[(List[Any], (Double, Double))]].map(v => (v._1, v._2._1 / v._2._2)) else union.asInstanceOf[RDD[(List[Any], Double)]]
  }

   /*
  * This rollup operator does not reuse the values obtained by other group by's
  * */
  def rollup_naive(dataset: RDD[Row], groupingAttributeIndexes: List[Int], aggAttributeIndex: Int, agg: String): RDD[(List[Any], Double)] = {
    val groups : List[RDD[(List[Any], Double)]] = 
        (-1 :: groupingAttributeIndexes.indices.toList).map(i => groupingAttributeIndexes.slice(0, i+1)).map(idx => groupBy(dataset, idx, aggAttributeIndex ,agg).persist).toList
    val union = groups.reduce(_ ++ _)
    union
  }
}

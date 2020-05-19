package rollup

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import scala.math._
import scala.{Double}

class RollupOperator() {
  // Performs a group by certain keys
  def groupBy(dataset: RDD[Row], grpIndex: List[Int], aggIndex: Int, agg: String): RDD[(List[Any], Double)] = {
    agg match {
      case "COUNT" => 
        val tupleSplit = (t : Row) => 
          if (t.isNullAt(aggIndex)) (grpIndex.map(i => t(i)), 0.0)
          else (grpIndex.map(i => t(i)), 1.0)
        val seqOp = (accumulator: Double, element: Double) =>  accumulator + element
        val combOp = (x: Double, y: Double) =>  x + y
        dataset.map(tupleSplit).aggregateByKey(0.0)(seqOp,seqOp) 
      case "SUM" => 
        def tupleSplit = (t : Row) => (grpIndex.map(i => t(i)),  t(aggIndex).toString().toDouble)
        def seqOp = (accumulator: Double, element: Double) =>  accumulator + element
        dataset.map(tupleSplit).aggregateByKey(0.0)(seqOp,seqOp) 
      case "MIN" => 
        def tupleSplit = (t : Row) => (grpIndex.map(i => t(i)),  t(aggIndex).toString().toDouble)
        def seqOp = (accumulator: Double, element: (Double)) =>  min(accumulator, element)
        dataset.map(tupleSplit).aggregateByKey(Double.MaxValue)(seqOp,seqOp) 
      case "MAX" => 
        def tupleSplit =  (t : Row) => (grpIndex.map(i => t(i)), if (t.isNullAt(aggIndex)) 0.0 else 1.0)
        def seqOp =(accumulator: Double, element: (Double)) =>  max(accumulator, element)
        dataset.map(tupleSplit).aggregateByKey(Double.MinValue)(seqOp,seqOp) 
      case "AVG" => 
        def tupleSplit = (t : Row) => (grpIndex.map(i => t(i)), (if (t.isNullAt(aggIndex)) (0.0, 0.0) else (t(aggIndex).toString().toDouble, 1.0)))
        def seqOp = (acc: (Double, Double), x: (Double, Double)) =>  (acc._1 + x._1, acc._2 + x._2)
        dataset.map(tupleSplit).aggregateByKey((0.0, 0.0))(seqOp,seqOp).map(v => (v._1, v._2._1 / v._2._2))
    }
  }

  // Performs a group by certain keys with aggregate average
  def groupByAVG(dataset: RDD[Row], grpIndex: List[Int], aggIndex: Int): RDD[(List[Any], (Double, Double))] = {
    def tupleSplit = (t : Row) => (grpIndex.map(i => t(i)), (if (t.isNullAt(aggIndex)) (0.0, 0.0) else (t(aggIndex).toString().toDouble, 1.0)))
    def seqOp = (acc: (Double, Double), x: (Double, Double)) =>  (acc._1 + x._1, acc._2 + x._2)
    dataset.map(tupleSplit).aggregateByKey((0.0, 0.0))(seqOp,seqOp)
  }

  // Do the next average roll up, group the groups according to the next key.
  def rollUpNextAvg(dataset: RDD[(List[Any], (Double, Double))]): RDD[(List[Any], (Double, Double))] = {
    val tupleSplit = (t : (List[Any], (Double, Double))) => (t._1.reverse.tail.reverse, t._2)
    val seqOp  = (acc: (Double, Double), x: (Double, Double)) =>  (acc._1 + x._1, acc._2 + x._2)
    dataset.map(tupleSplit).aggregateByKey((0.0, 0.0))(seqOp,seqOp) 
  }

  // Do the next roll up, group the groups according to the next key.
  def rollUpNext(dataset: RDD[(List[Any], Double)], agg: String): RDD[(List[Any], Double)] = {
    val normal = (t : (List[Any], Double)) => (t._1.reverse.tail.reverse, t._2)
    (agg match {
      case "COUNT" => 
        val seqOp = (accumulator: Double, element: Double) =>  accumulator + element
        dataset.map(normal).aggregateByKey(0.0)(seqOp, seqOp) 
      case "SUM" => 
        val seqOp = (accumulator: Double, element: Double) =>  accumulator + element
        dataset.map(normal).aggregateByKey(0.0)(seqOp, seqOp) 
      case "MIN" => 
        val seqOp = (accumulator: Double, element: (Double)) =>  min(accumulator, element)
        dataset.map(normal).aggregateByKey(Double.MaxValue)(seqOp, seqOp) 
      case "MAX" => 
        val seqOp =(accumulator: Double, element: (Double)) =>  max(accumulator, element)
        dataset.map(normal).aggregateByKey(Double.MinValue)(seqOp, seqOp) 
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
        val union = rollups.reduce(_ ++ _)
        union.count
        union
    }
  }

   /*
  * This rollup operator does not reuse the values obtained by other group by's
  * */
  def rollup_naive(dataset: RDD[Row], groupingAttributeIndexes: List[Int], aggAttributeIndex: Int, agg: String): RDD[(List[Any], Double)] = {
    val groups : List[RDD[(List[Any], Double)]] = 
        (-1 :: groupingAttributeIndexes.indices.toList).map(i => groupingAttributeIndexes.slice(0, i+1)).map(idx => groupBy(dataset, idx, aggAttributeIndex ,agg)).toList
    
    groups.foreach(_.count)
    val union = groups.reduce(_ ++ _)
    union
  }
}

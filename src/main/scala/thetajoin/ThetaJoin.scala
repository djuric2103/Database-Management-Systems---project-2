package thetajoin

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import org.slf4j.LoggerFactory
import scala.math._

class ThetaJoin(partitions: Int) extends java.io.Serializable {
  val logger = LoggerFactory.getLogger("ThetaJoin")
  class State
  case object No extends State
  case object Yes extends State
  case object Unsure extends State
  
  case class Borders(rows : List[Int], columns : List[Int], block_side : Double, cond : String) {
    /*
    Borders of the regions.

    If the sepparators of the rows are
      rows = List(4, 51, 156, 662)

    Then the regions are
      [-inf, 4), [4, 51), [51, 156), [156, 662), [662, inf)

    And we represent these regions using the borders
      regions = List(-inf, 4, 51, 156, 662, inf)

    To know which horizontal region an integer belongs, we fing its lower bound in the previous list
      val row : Int = 513
      val region_of_row : Int = lower_bound(regions, row) // = 156, thus the row is in [156, 662) 

    With this, each cell is a cartessian product of horizontal and vertical borders.
      val h_regions = List(-inf, 4, 51, 156, 662)
      val v_regions = List(-inf, 325, 6342, 15432, 110000)

      val regions = h_regions.cartessian(v_regions)
    
    And we know that all rows in a cell are bigger than every column in a cell if
      min(row) > max(col)

    Which is the same as checking that the lower bound of the horizontal region is bigger than the upper bound of the vertical region.
      
    If lower_bound(h_regions, row) > upper_bound(v_regions, col) then row > col
    */

    val new_rows = (Int.MaxValue :: (Int.MinValue :: rows).reverse).reverse
    val new_cols = (Int.MaxValue :: (Int.MinValue :: columns).reverse).reverse

    // Map from the lower bound borders of a cell to the cell index and state
    val regions : List[((Int, Int), (Int, State))] = {
          val seq = scala.collection.mutable.MutableList[((Int, Int), (Int, State))]()
          var i = 0
          var j = 0
          val row_blocks = (rows.length/block_side).toInt

          while (i < new_rows.length-1){
            while (j < new_cols.length-1){
              val b : State = cond match {
                  case ">" => 
                    if (new_rows(i + 1) > new_cols(j)) Yes
                    else if(new_rows(i) > new_cols(j+1)) No
                    else Unsure
                  case "<" => 
                    if (new_rows(i) > new_cols(j+1)) Yes
                    else if(new_rows(i + 1) > new_cols(j)) No
                    else Unsure
                }
              val elem : ((Int, Int), (Int, State)) =  ((new_rows(i), new_cols(i)), (i*row_blocks + j, b))
              seq += elem
              j += 1
            }
            i += 1
          }
          seq.toList
        }

    // Map from the horizontal borders to the regions it intersects with
    val left : Map[Int, Set[Int]] = (for(((r, c), (idx, state)) <- regions) yield {(idx, r)}).groupBy(_._1).mapValues(_.map(_._2).toSet.filter(i => region_map(i) != No))

    // Map from the vertical borders to the regions it intersects with
    val right : Map[Int, Set[Int]] = (for(((r, c), (idx, state)) <- regions) yield {(idx, c)}).groupBy(_._1).mapValues(_.map(_._2).toSet.filter(i => region_map(i) != No))

    // Map from the region index and the necessity of checking the tuple
    val region_map : Map[Int, State] = (for(((_, _), (idx, state)) <- regions) yield {(idx, state)}).toMap
    
    // Finds the lower bound of an integer in a list
    def lower_bound(x : Int, l : List[Int], ini : Int, end : Int) : Int = {
      val m = (ini + end)/2
      if (l(m) == x || (end - ini) < 2){
        var aux = end
        while (aux >= 0 && l(aux) > x)
          aux -= 1
        aux
      }else if(l(m) > x){
        lower_bound(x, l, ini, m)
      }else{
        lower_bound(x, l, m, end)
      }
    }

    // Gets all the regions that a column intersects
    def applyCol(x : Int) : Set[Int] = {
      right(lower_bound(x, Int.MinValue :: columns, 0, columns.length-1))
    }

    // Gets all the regions that a row intersects
    def applyRow(x : Int) : Set[Int] = {
      left(lower_bound(x, Int.MinValue :: rows, 0, rows.length-1))
    }
  }

  /*
  this method takes as input two datasets (dat1, dat2) and returns the pairs of join keys that satisfy the theta join condition.
  attrIndex1: is the index of the join key of dat1
  attrIndex2: is the index of the join key of dat2
  condition: Is the join condition. Assume only "<", ">" will be given as input
  Assume condition only on integer fields.
  Returns and RDD[(Int, Int)]: projects only the join keys.
   */
   def ineq_join(dat1: RDD[Row], dat2: RDD[Row], attrIndex1: Int, attrIndex2: Int, condition:String): RDD[(Int, Int)] = {
    val r = dat1.count()
    val s = dat2.count()
    val n = partitions
    val block = s*r/n
    
    val (n_left, n_right) = {
      val aux = sqrt(s*r/n)
      (round(r/aux).toInt, round(s/aux).toInt)
    }

    val rand = scala.util.Random
    val rows = dat1.takeSample(true, n_left-1).map(r => r(attrIndex2).asInstanceOf[Int])
    val cols = dat2.takeSample(true, n_right-1).map(r => r(attrIndex2).asInstanceOf[Int])

    val borders = Borders(rows.toList, cols.toList, sqrt(block), condition)

    val l_keys : RDD[(Int, Set[Int])] = dat1.map(x => {
      val aux = x(attrIndex1).asInstanceOf[Int]
      (aux, borders.applyRow(aux))
    })
    val r_keys : RDD[(Int, Set[Int])] = dat2.map(x => {
      val aux = x(attrIndex2).asInstanceOf[Int]
      (aux, borders.applyRow(aux))
    })

    val l_splits = (0 until n).map(i => l_keys.filter{
      x => x._2.contains(i)
    }.map(x => x._1))
    val r_splits = (0 until n).map(i => r_keys.filter{
      x => x._2.contains(i)
    }.map(x => x._1))
    
    val cart : IndexedSeq[RDD[(Int, Int)]] = (l_splits zip r_splits).map{case (a,b) => a.cartesian(b)}

    val result = cart.zipWithIndex.par
    // Prune unnecessary outputs
    // THIS SHOULD NOT BE NECESSARY. BORDERS SHOULD PRUNE THESE
    .filter{case (rdd, i) => 
      borders.region_map(i) match {
        case No => false
        case _ => true
      }
    }
    // Prune unnecessary condition checks
    .map{case (rdd, i) => {
      borders.region_map(i) match {
        case Yes => rdd
        case Unsure => 
          condition match {
            case ">"=> rdd.filter{case (x, y) => x > y}
            case "<"=> rdd.filter{case (x, y) => x < y}
          }
      }
    }}.toList

    result.reduce(_ ++ _)
  }
}

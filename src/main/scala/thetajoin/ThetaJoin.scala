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

   def ineq_join(dat1: RDD[Row], dat2: RDD[Row], attrIndex1: Int, attrIndex2: Int, condition:String): RDD[(Int, Int)] = {
    val r = dat1.count()
    val s = dat2.count()
    val n = partitions
    val block = s*r/n
    // println(s"Block size: $block")
    val (n_left, n_right) = {
      val aux = sqrt(s*r/n)
      (round(r/aux).toInt, round(s/aux).toInt)
    }
    // println(s"Left: $n_left, Right: $n_right")

    val rand = scala.util.Random
    val rows = dat1.takeSample(true, n_left-1).map(r => r(attrIndex2) match {
      case x : Int => x
      case x : Double => x.toInt
      case x : Float => x.toInt
      case x : String => x.toInt
    })
    val cols = dat2.takeSample(true, n_right-1).map(r => r(attrIndex2) match {
      case x : Int => x
      case x : Double => x.toInt
      case x : Float => x.toInt
      case x : String => x.toInt
    })
    
    val new_rows = (Int.MaxValue :: (Int.MinValue :: rows.toList).reverse).reverse
    val new_cols = (Int.MaxValue :: (Int.MinValue :: cols.toList).reverse).reverse

    // Map from the lower bound borders of a cell to the cell index and state
    val block_side = sqrt(block)
    val regions : List[((Int, Int), (Int, State))] = {
          val seq = scala.collection.mutable.MutableList[((Int, Int), (Int, State))]()
          var i = 0
          var j = 0
          val row_blocks = (rows.length/block_side).toInt

          // println(s"Rows: $new_rows")
          // println(s"Cols: $new_cols")

          while (i < new_rows.length-1){
            while (j < new_cols.length-1){
              val b : State = condition match {
                  case ">" => 
                    val aux = 
                    if (new_rows(i) > new_cols(j + 1)-1) Yes
                    else if(new_rows(i+1)-1 <= new_cols(j)) No
                    else Unsure
                    // println(s"($i > $j) [${new_rows(i)},${new_rows(i + 1)}]x[${new_cols(j)},${new_cols(j+1)}] => $aux")
                    aux
                  case "<" => 
                    val aux = if (new_rows(i + 1)-1 < new_cols(j)) Yes
                    else if(new_rows(i) >= new_cols(j+1)-1) No
                    else Unsure
                    // println(s"($i < $j) [${new_rows(i)},${new_rows(i + 1)}]x[${new_cols(j)},${new_cols(j+1)}] => $aux")
                    aux
                }
              val elem : ((Int, Int), (Int, State)) =  ((new_rows(i), new_cols(j)), (i*(new_rows.length-1) + j, b))
              // println(elem)
              seq += elem
              j += 1
            }
            i += 1
            j = 0
          }
          seq.toList
        }
    
    // Map from the region index and the necessity of checking the tuple
    val region_map : Map[Int, State] = {
      (for(((_, _), (idx, state)) <- regions) yield {(idx, state)}).toMap
    }

    // val i2left : Map[Int, Set[Int]] = (for(((r, c), (idx, state)) <- regions) yield {(idx, r)})
    //   .groupBy(_._1).mapValues{_.map(_._2).toSet}
    //   .filter{case (i, s) => 
    //     region_map(i) != No
    //   }
    val left2i : Map[Int, Set[Int]] = (for(((r, c), (idx, state)) <- regions) yield {(r, idx)})
      .groupBy(_._1).mapValues{_.map(_._2).toSet}
      .map{case (i, s) => 
        (i, s.filter(i => region_map(i) != No))
      }
    // val i2right : Map[Int, Set[Int]] = (for(((r, c), (idx, state)) <- regions) yield {(idx, c)})
    //   .groupBy(_._1).mapValues{_.map(_._2).toSet}
    //   .filter{case (i, s) => 
    //     region_map(i) != No
    //   }
    val right2i : Map[Int, Set[Int]] = (for(((r, c), (idx, state)) <- regions) yield {(c, idx)})
      .groupBy(_._1).mapValues{_.map(_._2).toSet}
      .map{case (i, s) => 
        (i, s.filter(i => region_map(i) != No))
      }

    val r_keys : RDD[(Int, Set[Int])] = dat2.mapPartitions(part => {
      def lower_bound(x : Int, l : List[Int], ini : Int, end : Int) : Int = {
        val m = (ini + end)/2
        if (l(m) == x || (end - ini) < 2){
          var aux = end
          while (aux >= 0 && l(aux) > x)
            aux -= 1
          l(aux)
        }else if(l(m) > x){
          lower_bound(x, l, ini, m)
        }else{
          lower_bound(x, l, m, end)
        }
      }

      part.map(t => {
        val aux = t(attrIndex1) match {
          case x : Int => x
          case x : Double => x.toInt
          case x : Float => x.toInt
          case x : String => x.toInt
        }
        (aux, right2i(lower_bound(aux, Int.MinValue :: cols.toList, 0, cols.length)))
      })
    })

    val l_keys : RDD[(Int, Set[Int])] = dat1.mapPartitions{part => 
      def lower_bound(x : Int, l : List[Int], ini : Int, end : Int) : Int = {
        val m = (ini + end)/2
        if (l(m) == x || (end - ini) < 2){
          var aux = end
          while (aux >= 0 && l(aux) > x)
            aux -= 1
          l(aux)
        }else if(l(m) > x){
          lower_bound(x, l, ini, m)
        }else{
          lower_bound(x, l, m, end)
        }
      }

      part.map(t => {
        val aux = t(attrIndex1) match {
          case x : Int => x
          case x : Double => x.toInt
          case x : Float => x.toInt
          case x : String => x.toInt
        }
        (aux, left2i(lower_bound(aux, Int.MinValue :: rows.toList, 0, rows.length)))
      })
    }

    val valid_indices = (0 until n).filter{i => region_map(i) != No}
    val l_splits = valid_indices.map(i => l_keys.filter{x => x._2.contains(i)}.map(x => x._1))
    val r_splits = valid_indices.map(i => r_keys.filter{x => x._2.contains(i)}.map(x => x._1))

    val cart : IndexedSeq[RDD[(Int, Int)]] = (l_splits zip r_splits).map{case (a,b) => a.cartesian(b)}

    val result = cart.zip(valid_indices).par
    // Prune unnecessary condition checks
    .map{case (rdd, i) => {
      region_map(i) match {
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

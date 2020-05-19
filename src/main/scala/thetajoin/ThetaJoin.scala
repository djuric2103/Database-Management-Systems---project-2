package thetajoin

import org.apache.spark.rdd.RDD
import org.apache.spark.Partitioner
import org.apache.spark.sql.Row
import org.slf4j.LoggerFactory
import scala.math._

class ThetaJoin(partitions: Int) extends java.io.Serializable {
  // val logger = LoggerFactory.getLogger("ThetaJoin").setLevel(Level.ERROR)
  
  class State extends java.io.Serializable 
  case object No extends State
  case object Yes extends State
  case object Unsure extends State

  def this() = this(4)

  def toInt(value : Any) : Int = {
      value match {
        case x : Int => x
        case x : Double => x.toInt
        case x : Float => x.toInt
        case x : String => x.toInt
        case x => x.toString().toInt
      }
    }

   def ineq_join(dat1: RDD[Row], dat2: RDD[Row], attrIndex1: Int, attrIndex2: Int, condition:String): RDD[(Int, Int)] = {
    val r = dat1.count()
    val s = dat2.count()
    val n : Int = partitions
    val block : Double = (s*r).toDouble/n
    val n_real_left : Double = sqrt((n*s).toDouble/r)
    val n_real_right : Double = sqrt((n*r).toDouble/s)
    
    // In case the partitions do not match well, approximate the problem.
    val n_left = n_real_left.toInt + (if (n_real_left % 1 != 0) 1 else 0)
    val n_right = n_real_right.toInt + (if (n_real_right % 1 != 0) 1 else 0)

    val rand = scala.util.Random
    val rows = dat1.takeSample(true, n_left-1).map(r => toInt(r(attrIndex1)))
    val cols = dat2.takeSample(true, n_right-1).map(r => toInt(r(attrIndex2)))
    
    val lower_rows = (Int.MinValue :: rows.toList)
    val lower_cols = (Int.MinValue :: cols.toList)
    val new_rows = (Int.MaxValue :: lower_rows.reverse).reverse
    val new_cols = (Int.MaxValue :: lower_cols.reverse).reverse
    
    // Map from the lower bound borders of a cell to the cell index and state
    val block_side = sqrt(block)
    val regions : List[((Int, Int), (Int, State))] = {
          var seq = List[((Int, Int), (Int, State))]()
          var i = 0
          var j = 0
          val row_blocks = (rows.length/block_side).toInt

          while (i < new_rows.length-1){
            while (j < new_cols.length-1){
              val b : State = condition match {
                  case ">" => 
                    if (new_rows(i) > new_cols(j + 1)-1) Yes
                    else if(new_rows(i+1)-1 <= new_cols(j)) No
                    else Unsure
                  case "<" => 
                    if (new_rows(i + 1)-1 < new_cols(j)) Yes
                    else if(new_rows(i) >= new_cols(j+1)-1) No
                    else Unsure
                }
              val elem : ((Int, Int), (Int, State)) =  ((new_rows(i), new_cols(j)), (i*(new_cols.length-1) + j, b))
              seq = elem :: seq
              j += 1
            }
            i += 1
            j = 0
          }
          seq.toList
        }

    // Map from the region index and the necessity of checking the tuple
    
    val region_map : Map[Int, State] = (for(((_, _), (idx, state)) <- regions) yield {(idx, state)}).toMap
    val left2i : Map[Int, Set[Int]] = (for(((r, c), (idx, state)) <- regions) yield {(r, idx)}).groupBy(_._1).map{case (k,v) => (k, v.map(_._2).filter(i => region_map(i) != No).toSet)}
    val right2i : Map[Int, Set[Int]] = (for(((r, c), (idx, state)) <- regions) yield {(c, idx)}).groupBy(_._1).map{case (k,v) => (k, v.map(_._2).filter(i => region_map(i) != No).toSet)}
    
    val partitioner = new Partitioner {
      def numPartitions: Int = n
      def getPartition(key: Any): Int = key.asInstanceOf[Int] % n
    }

    val l_keys : RDD[(Int, Int)] = dat1.mapPartitions(partition => {

      def lower_bound(x : Int, l : List[Int], ini : Int, end : Int) : Int = {
        val m = (ini + end)/2
        if (l(m) == x || (end - ini) < 2){
          var aux_end = end
          while (aux_end >= 0 && l(aux_end) > x)
            aux_end -= 1
          aux_end
        }else if(l(m) > x){
          lower_bound(x, l, ini, m)
        }else{
          lower_bound(x, l, m, end)
        }
      }

      partition.flatMap{t => 
        val aux = toInt(t(attrIndex1))
        val index = lower_rows(lower_bound(aux, lower_rows, 0, rows.length))
        val indices = left2i(index)
        indices.map(i => (i->aux))
      }
    })//.partitionBy(partitioner)
    
    val l_splits : RDD[(Int, Iterable[Int])] = l_keys.groupByKey()
    val r_keys : RDD[(Int, Int)] = dat2.mapPartitions(partition => {

      def lower_bound(x : Int, l : List[Int], ini : Int, end : Int) : Int = {
        val m = (ini + end)/2
        if (l(m) == x || (end - ini) < 2){
          var aux_end = end
          while (aux_end >= 0 && l(aux_end) > x)
            aux_end -= 1
          aux_end
        }else if(l(m) > x){
          lower_bound(x, l, ini, m)
        }else{
          lower_bound(x, l, m, end)
        }
      }
      
      partition.flatMap{t => 
        val aux = toInt(t(attrIndex2))
        val index = lower_bound(aux, lower_cols, 0, cols.length)
        val indices = right2i(lower_cols(index))
        indices.map(i => (i->aux))
      }
    })
    val r_splits : RDD[(Int, Iterable[Int])] = r_keys.groupByKey()
    val join : RDD[(Int, (Iterable[Int], Iterable[Int]))] = l_splits.join(r_splits)
    val partitioned : RDD[(Int, (Iterable[Int], Iterable[Int]))] = join.partitionBy(partitioner)
    val blocks : RDD[(State, (Iterable[Int], Iterable[Int]))] = partitioned.map{case (k, (a,b)) => (region_map(k), (a,b))}

    val (_,(a,b)) = (Unsure,(List(1, 2, 3),List(0, 2, 7, 9, 12, 18, 4)))
    def eval : (Int, Int) => Boolean = (l : Int, r : Int) => l < r 
    val aux = a.flatMap(x => b.flatMap{y => if (eval(x,y)) Some(x,y) else None})

    // println(s"${blocks.count} blocks evaluated")
    val result : RDD[(Int, Int)] = blocks.mapPartitions{partition => {
      if (partition.hasNext){
      val (state, (a, b)) = partition.next
      
      def evaluate : (Int, Int) => Boolean = condition match {
            case ">" => (l : Int, r : Int) => l > r 
            case "<" => (l : Int, r : Int) => l < r 
          }
  
      (state match {
        case Yes => a.flatMap(x => b.map(y => (x,y)))
        case _ => a.flatMap(x => b.flatMap{y => if (evaluate(x,y)) Some(x,y) else None})
      }).iterator
      
      } else List[(Int,Int)]().iterator
    }}
    result.count
    result
  }
}
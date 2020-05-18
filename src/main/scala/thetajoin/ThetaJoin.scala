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
    
    // println(region_map)
    // println(left2i)
    // println(right2i)
    val partitioner = new Partitioner {
      def numPartitions: Int = n
      def getPartition(key: Any): Int = key.asInstanceOf[Int] % n
    }

    // val l_keys : RDD[(Int, Int)] = dat2.flatMap(t => {
    //   val aux = toInt(t(attrIndex1))
    //   val indices = right2i(lower_rows(lower_bound(aux, lower_rows, 0, rows.length)))
    //   indices.map(i => (i->aux))
    // })//.partitionBy(partitioner)
    // println("Evaluating left")

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
        // println(s"Going to search for ${aux} in ${lower_rows} in ${left2i}")
        val indices = left2i(index)
        indices.map(i => (i->aux))
      }
    })//.partitionBy(partitioner)
    
    val l_splits : RDD[(Int, Iterable[Int])] = l_keys.groupByKey()
    // l_splits.count
    // println("Left evaluated")
    // println("Evaluating right")
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
        // println(s"Going to search for ${aux} in ${lower_cols} in ${right2i}")
        val indices = right2i(lower_cols(index))
        indices.map(i => (i->aux))
      }
    })//.partitionBy(partitioner)
    
    val r_splits : RDD[(Int, Iterable[Int])] = r_keys.groupByKey()
    // r_splits.count
    // println("Right evaluated")
    // println("Right")
    // l_splits.collect.foreach(println)
    // println("Left")
    // r_splits.collect.foreach(println)
    
    val join : RDD[(Int, (Iterable[Int], Iterable[Int]))] = l_splits.join(r_splits)
    // join.count
    // println("Join")
    // join.collect.foreach(println)

    val partitioned : RDD[(Int, (Iterable[Int], Iterable[Int]))] = join.partitionBy(partitioner)
    
    // partitioned.count
    // println("Partitioning")
    // partitioned.collect.foreach(println) //foreachPartition{p => p.toList.foreach(println)}
    // partitioned.map{case (k, (a,b)) => k}.collect.foreach{k => println(s"Going to search for ${k} in ${region_map}")}
    val blocks : RDD[(State, (Iterable[Int], Iterable[Int]))] = partitioned.map{case (k, (a,b)) => (region_map(k), (a,b))}
    // blocks.count
    // println("Blocks")
    // blocks.collect.foreach(println)

    val (_,(a,b)) = (Unsure,(List(1, 2, 3),List(0, 2, 7, 9, 12, 18, 4)))
    def eval : (Int, Int) => Boolean = (l : Int, r : Int) => l < r 
    val aux = a.flatMap(x => b.flatMap{y => if (eval(x,y)) Some(x,y) else None})

    // println(s"${blocks.count} blocks evaluated")
    val result : RDD[(Int, Int)] = blocks.mapPartitions{partition => {
      if (partition.hasNext){
      // assert(partition.hasNext,  "Impossible")
      // println("Partition:")
      // partition.toList.foreach(println)
      // println("Partition...")
      val (state, (a, b)) = partition.next
      // println((a.toList, b.toList))
      
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
//     val r = dat1.count()
//     val s = dat2.count()
//     val n : Int = partitions
//     val block : Double = s*r/n
//     val n_real_left : Double = sqrt((n*s)/r)
//     val n_real_right : Double = sqrt((n*r)/s)
    
//     // In case the partitions do not match well, approximate the problem.
//     val n_left = n_real_left.toInt + (if (n_real_left % 1 != 0) 1 else 0)
//     val n_right = n_real_right.toInt + (if (n_real_right % 1 != 0) 1 else 0)

//     val rand = scala.util.Random
//     val rows = dat1.takeSample(true, n_left-1).map(r => toInt(r(attrIndex1)))
//     val cols = dat2.takeSample(true, n_right-1).map(r => toInt(r(attrIndex2)))
    
//     val lower_rows = (Int.MinValue :: rows.toList)
//     val lower_cols = (Int.MinValue :: cols.toList)
//     val new_rows = (Int.MaxValue :: lower_rows.reverse).reverse
//     val new_cols = (Int.MaxValue :: lower_cols.reverse).reverse
    
    
    
//     // Map from the lower bound borders of a cell to the cell index and state
//     val block_side = sqrt(block)
//     val regions : List[((Int, Int), (Int, State))] = {
//           val seq = scala.collection.mutable.MutableList[((Int, Int), (Int, State))]()
//           var i = 0
//           var j = 0
//           val row_blocks = (rows.length/block_side).toInt

//           while (i < new_rows.length-1){
//             while (j < new_cols.length-1){
//               val b : State = condition match {
//                   case ">" => 
//                     if (new_rows(i) > new_cols(j + 1)-1) Yes
//                     else if(new_rows(i+1)-1 <= new_cols(j)) No
//                     else Unsure
//                   case "<" => 
//                     if (new_rows(i + 1)-1 < new_cols(j)) Yes
//                     else if(new_rows(i) >= new_cols(j+1)-1) No
//                     else Unsure
//                 }
//               val elem : ((Int, Int), (Int, State)) =  ((new_rows(i), new_cols(j)), (i*(new_cols.length-1) + j, b))
//               seq += elem
//               j += 1
//             }
//             i += 1
//             j = 0
//           }
//           seq.toList
//         }
      
    

//     // Map from the region index and the necessity of checking the tuple
//     val region_map : Map[Int, State] = (for(((_, _), (idx, state)) <- regions) yield {(idx, state)}).toMap
//     val left2i : Map[Int, Set[Int]] = (for(((r, c), (idx, state)) <- regions) yield {(r, idx)}).groupBy(_._1).mapValues{_.map(_._2).toSet}.map{case (i, s) => (i, s.filter(i => region_map(i) != No))}
//     val right2i : Map[Int, Set[Int]] = (for(((r, c), (idx, state)) <- regions) yield {(c, idx)}).groupBy(_._1).mapValues{_.map(_._2).toSet}.map{case (i, s) => (i, s.filter(i => region_map(i) != No))}
    
//     val partitioner = new Partitioner {
//       def numPartitions: Int = n
//       def getPartition(key: Any): Int = key.asInstanceOf[Int]
//     }

//     // val l_keys : RDD[(Int, Int)] = dat2.flatMap(t => {
//     //   val aux = toInt(t(attrIndex1))
//     //   val indices = right2i(lower_rows(lower_bound(aux, lower_rows, 0, rows.length)))
//     //   indices.map(i => (i->aux))
//     // })//.partitionBy(partitioner)
//     println("Evaluating left")
//     val l_keys : RDD[(Int, Int)] = dat2.mapPartitions(partition => {

//       def lower_bound(x : Int, l : List[Int], ini : Int, end : Int) : Int = {
//         val m = (ini + end)/2
//         if (l(m) == x || (end - ini) < 2){
//           var aux_end = end
//           while (aux_end >= 0 && l(aux_end) > x)
//             aux_end -= 1
//           aux_end
//         }else if(l(m) > x){
//           lower_bound(x, l, ini, m)
//         }else{
//           lower_bound(x, l, m, end)
//         }
//       }

//       partition.flatMap{t => 
//         val aux = t(attrIndex1) match {
//           case x : Int => x
//           case x : Double => x.toInt
//           case x : Float => x.toInt
//           case x : String => x.toInt
//           case x => x.toString().toInt
//         }
//         val indices = right2i(lower_rows(lower_bound(aux, lower_rows, 0, rows.length)))
//         indices.map(i => (i->aux))
//       }
//     })//.partitionBy(partitioner)
//     l_keys.count()
//     println("Left evaluated")
//     val l_splits : RDD[(Int, Iterable[Int])] = l_keys.groupByKey()
    
//     val r_keys : RDD[(Int, Int)] = dat2.mapPartitions(partition => {

//       def lower_bound(x : Int, l : List[Int], ini : Int, end : Int) : Int = {
//         val m = (ini + end)/2
//         if (l(m) == x || (end - ini) < 2){
//           var aux_end = end
//           while (aux_end >= 0 && l(aux_end) > x)
//             aux_end -= 1
//           aux_end
//         }else if(l(m) > x){
//           lower_bound(x, l, ini, m)
//         }else{
//           lower_bound(x, l, m, end)
//         }
//       }
      
//       partition.flatMap{t => 
//         val aux = t(attrIndex2) match {
//           case x : Int => x
//           case x : Double => x.toInt
//           case x : Float => x.toInt
//           case x : String => x.toInt
//           case x => x.toString().toInt
//         }
//         val indices = right2i(lower_cols(lower_bound(aux, lower_cols, 0, cols.length)))
//         indices.map(i => (i->aux))
//       }
//     })//.partitionBy(partitioner)
//     val r_splits : RDD[(Int, Iterable[Int])] = r_keys.groupByKey()

//     val blocks : RDD[(Int, (Iterable[Int], Iterable[Int]))] = l_splits.join(r_splits).partitionBy(partitioner)

//     val result : RDD[(Int, Int)] = blocks.mapPartitions{partition => {
//       val key : Int = partition.map{case (x, (l,r)) => x}.take(1).toList.head
//       val left : Iterator[Int] = partition.flatMap{case (x, (l,r)) => l}
//       val right : Iterator[Int] = partition.flatMap{case (x, (l,r)) => r}
//       def evaluate : (Int, Int) => Boolean = condition match {
//             case ">" => (l : Int, r : Int) => l > r 
//             case "<" => (l : Int, r : Int) => l < r 
//           }
//       var res = List[(Int, Int)]()
//       region_map(key) match {
//         case Yes => {
//           // Doesnt have to evaluate
//           while(left.hasNext){
//             val l = left.next
//             while(right.hasNext){
//               val r = right.next
//               res .::= (l, r)
//             }
//           }
//         }
//         case _ => {
//           // Has to evaluate
//           while(left.hasNext){
//             val l = left.next
//             while(right.hasNext){
//               val r = right.next
//               if (evaluate(l,r))
//                 res .::= (l, r)
//               }
//             }
//           }
//         }
        
//       res.iterator
//     }}
//     result.count
//     result
//   }
// }

  //   // Map from the region index and the necessity of checking the tuple
  //   val region_map : Map[Int, State] = {
  //     (for(((_, _), (idx, state)) <- regions) yield {(idx, state)}).toMap
  //   }
    
  //   val left2i : Map[Int, Set[Int]] = (for(((r, c), (idx, state)) <- regions) yield {(r, idx)})
  //     .groupBy(_._1).mapValues{_.map(_._2).toSet}
  //     .map{case (i, s) => 
  //       (i, s.filter(i => region_map(i) != No))
  //     }
      
  //   val right2i : Map[Int, Set[Int]] = (for(((r, c), (idx, state)) <- regions) yield {(c, idx)})
  //     .groupBy(_._1).mapValues{_.map(_._2).toSet}
  //     .map{case (i, s) => 
  //       (i, s.filter(i => region_map(i) != No))
  //     }

  //   val r_keys : RDD[(Int, Set[Int])] = dat2.mapPartitions(part => {
  //     // Definition has to be declared here due to serialization issues
  //     def lower_bound(x : Int, l : List[Int], ini : Int, end : Int) : Int = {
  //       val m = (ini + end)/2
  //       if (l(m) == x || (end - ini) < 2){
  //         // Find the maximum index i with l(i) <= x
  //         var aux_end = end
  //         while (aux_end >= 0 && l(aux_end) > x)
  //           aux_end -= 1
  //         aux_end
  //         // This is in case there are several repeated elements in the list
  //         // Fins the minimum index i with l(i) >= x
  //         // var aux_beginning = aux_end
  //         // while (l(aux_beginning-1) == x)
  //         //   aux_beginning -= 1
  //         // (random()*(aux_end - aux_beginning) + aux_beginning).toInt
  //       }else if(l(m) > x){
  //         lower_bound(x, l, ini, m)
  //       }else{
  //         lower_bound(x, l, m, end)
  //       }
  //     }

  //     val list = Int.MinValue :: cols.toList
  //     part.map(t => {
  //       val aux = t(attrIndex2)match {
  //         case x : Int => x
  //         case x : Double => x.toInt
  //         case x : Float => x.toInt
  //         case x : String => x.toInt
  //         case x => x.toString().toInt
  //       }
  //       (aux, right2i(list(lower_bound(aux, list, 0, cols.length))))
  //     })
  //   })

  //   val l_keys : RDD[(Int, Set[Int])] = dat1.mapPartitions{part => 
  //     // Definition has to be declared here
  //     def lower_bound(x : Int, l : List[Int], ini : Int, end : Int) : Int = {
  //       val m = (ini + end)/2
  //       if (l(m) == x || (end - ini) < 2){
  //         var aux = end
  //         while (aux >= 0 && l(aux) > x)
  //           aux -= 1
  //         aux
  //       }else if(l(m) > x){
  //         lower_bound(x, l, ini, m)
  //       }else{
  //         lower_bound(x, l, m, end)
  //       }
  //     }

  //     val list = Int.MinValue :: rows.toList
  //     part.map(t => {
  //       val aux = t(attrIndex1)match {
  //         case x : Int => x
  //         case x : Double => x.toInt
  //         case x : Float => x.toInt
  //         case x : String => x.toInt
  //         case x => x.toString().toInt
  //       }
  //       (aux, left2i(list(lower_bound(aux, list, 0, rows.length))))
  //     })
  //   }

  //   // println("Reached cartessian product")
  //   val valid_indices = (0 until n).filter{i => region_map(i) != No}
  //   val l_splits = valid_indices.map(i => l_keys.filter{x => x._2.contains(i)}.map(x => x._1))
  //   val r_splits = valid_indices.map(i => r_keys.filter{x => x._2.contains(i)}.map(x => x._1))
  //   val cart : IndexedSeq[RDD[(Int, Int)]] = (l_splits zip r_splits).map{case (a,b) => a.cartesian(b)}
    
  //   // Prune unnecessary condition checks
  //   val result = cart.zip(valid_indices)
  //   .map{case (rdd, i) => {
  //     region_map(i) match {
  //       case Yes => rdd.map{case x => (i % n -> x)}
  //       case Unsure => 
  //         condition match {
  //           case ">" => rdd.flatMap{case (x, y) => if (x > y) Some(i%n -> (x,y)) else None}
  //           case "<" => rdd.flatMap{case (x, y) => if (x < y) Some(i%n -> (x,y)) else None}
  //         }
  //     }
  //   }}.toList

  //   result.reduce(_ ++ _).partitionBy(new Partitioner {
  //     def numPartitions: Int = n
  //     def getPartition(key: Any): Int = key.asInstanceOf[Int]
  //   }).map{case (k, v) => v}
  // }
// }

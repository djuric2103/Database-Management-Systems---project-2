package lsh

import org.apache.spark.rdd.RDD

class ANDConstruction(children: List[Construction]) extends Construction {
  override def eval(rdd: RDD[(String, List[String])]): RDD[(String, Set[String])] = {
    /*
    * Implement an ANDConstruction for one or more LSH constructions (simple or composite)
    * children: LSH constructions to compose
    * rdd: data points in (movie_name, [keyword_list]) format that represent the queries
    * return near-neighbors in (movie_name, [nn_movie_names]) as an RDD[(String, Set[String])]
    * */
    var neighbour: RDD[(String, Set[String])] = null;
    for (h <- children) {
      if (neighbour == null) neighbour = h.eval(rdd)
      else neighbour = neighbour
        .join(h.eval(rdd))
        .map(x => (x._1, x._2._1.intersect(x._2._2)))
    //  neighbour.sortBy(_._2.size).foreach(x => print(x._1 + "\t" +x._2.size + "\t"))
     // println()

    }
    return neighbour
  }
}

package lsh

object Randomized {
  val r = new util.Random()
  def getNext() : Int ={
    return r.nextInt()
  }
}

package co.okulab.trainingTest

object TestApp {

  def divideVar(a: Int, b: Int): Int ={
    a/b
  }


  def main(args: Array[String]): Unit = {

    println("dividing : " + divideVar(30,10))
  }

}

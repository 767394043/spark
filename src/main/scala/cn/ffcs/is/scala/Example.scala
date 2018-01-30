package cn.ffcs.is.scala

import java.io.File
import java.util
import java.util.{Calendar, Comparator, NoSuchElementException, Scanner}

import scala.util.Random
import scala.BigInt._
import scala.beans.BeanProperty
import scala.collection.mutable.ArrayBuffer
import scala.collection.{JavaConversions, mutable}
object Example {

  def main(args: Array[String]): Unit = {
    five()
  }

  def one() : Unit = {
    val res1 = math.sqrt(3)
    val res2 = math.pow(res1,2)
    println(3 - res2)
    println("infi"*3)
    BigInt(math.pow(2,1024).toInt)

    //import scala.util.Random
    //import scala.BigInt._
    probablePrime(100,Random)
    println("infi"(0))
    println("infi1"last)
    println("infi1".take(2))
    println("infi1".drop(1))
    println("infi1".takeRight(1))
    println("infi1".dropRight(1))

  }

  def two() : Unit = {
    println(if (1 > 2) 1 else 0)
    for (i <- 1 to 10) {
      print(i + "|")

    }
    println()

    for (i <- 0 to 10; j <- 0 to 10) {
      print(i + "-" + j + "|")
    }
    println()

    for (i <- 0 to 10; j <- 0 to 10; if i == j) {
      print(i + "-" + j + "|")
    }
    println()

    for (i <- 0 to 10; from = 4 - i; j <- from to 10) {
      print(i + "-" + j + "|")
    }
    println()

    val indexedSeqInt: IndexedSeq[Int] = for (i <- 0 to 10) yield i % 3
    indexedSeqInt.foreach(tuple => print(tuple + "|"))
    println(indexedSeqInt)

    val indexedSeqChar: IndexedSeq[Char] = for (i <- 0 to 10) yield (i % 3 + 'a').toChar
    indexedSeqChar.foreach(tuple => print(tuple + "|"))
    println()

    val result1 = for (c <- "infi"; i <- 1 to 10) yield c + i
    println(result1)

    val result2 = for (c <- "infi"; i <- 1 to 10) yield (c + i).toChar
    println(result2)

    val result3 = for (i <- 1 to 10; c <- "infi") yield c + i
    println(result3)

    val result4 = for (i <- 1 to 10; c <- "infi") yield (c + i).toChar
    println(result4)

    def decorate(string: String, left: String = "[", right: String = "]"): String = {
      left + string + right
    }

    println(decorate("infi"))
    println(decorate("infi", "<", ">"))
    println(decorate(left = "<", string = "infi", right = ">"))

    def sum(args: Int*): Int = {
      var sum = 0;
      for (arg <- args) {
        sum += arg
      }
      sum
    }

    println(sum(1, 2, 3, 4))
    println(sum(1 to 4: _*))

    //到这里执行
    val wordsVal = scala.io.Source.fromFile("/Users/chenwei/Downloads/sparkTest.txt").mkString
    //访问wordsLazyVal执行
    lazy val wordsLazyVal = scala.io.Source.fromFile("/Users/chenwei/Downloads/sparkTest.txt").mkString

    //每次访问wordsDef执行
    def wordsDef = scala.io.Source.fromFile("/Users/chenwei/Downloads/sparkTest.txt").mkString

    if (1 > 0) {
      math.sqrt(1)
    } else {
      throw new IllegalArgumentException("x shouble no be 0")
    }

    def getSignum(int: Int): Int = {
      if (int > 0) {
        1
      } else if (int == 0) {
        0
      } else {
        -1
      }
    }

    println(getSignum(0))

    var y = 0
    var x: Unit = y = 1

    for (i <- 10 to 0 by -1) {
      println(i)
    }

    def countdown(n: Int): Unit = {
      for (i <- n to 1 by -1) {
        println(i)
      }
    }

    countdown(2)


    var sumStr = 1
    for (ch <- "infi") {
      sumStr *= ch
    }
    println(sumStr)

    sumStr = 1
    "infi".foreach(sumStr *= _)
    println(sumStr)

    def product(string: String): Int = {
      var sumStr = 1
      string.foreach(sumStr *= _)
      sumStr
    }

    def productRecursive(string: String): Int = {
      if (string.length == 1) {
        string.take(1).charAt(0)
      } else {
        string.take(1).charAt(0) * productRecursive(string.drop(1))
      }
    }

    println(product("infi"))
    println(productRecursive("infi"))

    def defPow(x : Double, n : Int) : Double ={

      if (n == 0){
        1.0
      }else if (n < 0){
        1 / defPow(x,-1 * n)
      }else if(n > 0 && n %2 == 0){
        defPow(x,n/2) * defPow(x,n/2)
      }else{
        x * defPow(x,n - 1)
      }

    }

  }

  def three() : Unit = {
    val nums = new Array[Int](10)
    val arrayBuffer = ArrayBuffer[Int]()
    arrayBuffer += 1
    println(arrayBuffer)
    arrayBuffer += (2,3,4,5)
    println(arrayBuffer)
    arrayBuffer ++= Array(6,7,8)
    println(arrayBuffer)
    arrayBuffer.trimEnd(3)
    println(arrayBuffer)
    arrayBuffer.insert(0,0)
    println(arrayBuffer)
    arrayBuffer.insert(0,-1,-2,-3)
    println(arrayBuffer)
    arrayBuffer.remove(0)
    println(arrayBuffer)
    arrayBuffer.remove(0,2)
    println(arrayBuffer)


    for (i <- arrayBuffer.indices){
      print(i + "-" + arrayBuffer(i) + "|")
    }
    println()
    val newarrayBuffer = arrayBuffer.map(2 * _)
    for (i <- newarrayBuffer){
      print(i + "|")
    }
    println()

    def first() : Unit ={
      val arrayBuffer = new ArrayBuffer[Int]
      arrayBuffer += (-3,-2,-1,0,1,2,3,4)
      var isFirst = true
      var length = arrayBuffer.length
      var i = 0
      while (i < length){
        if (arrayBuffer(i) < 0){
          if (isFirst){
            isFirst = false
          }else{
            arrayBuffer.remove(i)
            i = i - 1
            length = length - 1
          }
        }
        i = i + 1
      }

      println(arrayBuffer)

    }

    first()

    def first2() : Unit = {
      val arrayBuffer = new ArrayBuffer[Int]
      arrayBuffer += (-3,-2,-1,0,1,2,3,4)
      var isFirst = true
      val indexs = for (i <- arrayBuffer.indices if isFirst || arrayBuffer(i) >= 0) yield {
        if (isFirst){
          isFirst = false
        }
        i
      }

      for (j <- indexs.indices){
        arrayBuffer(j) = arrayBuffer(indexs(j))
      }
      arrayBuffer.trimEnd(arrayBuffer.length - indexs.length)
      println(arrayBuffer)
    }

    first2()

    println(arrayBuffer.sum)
    println(arrayBuffer.max)
    println(arrayBuffer.min)

    val array = Array(4,3,2,1)
    println()
    array.foreach(print)
    scala.util.Sorting.quickSort(array)
    println()
    array.foreach(print)
    println()
    println(array.mkString("<",",",">"))

    val matrix = Array.ofDim[Int](2,2)
    matrix.foreach(tuple => {tuple.foreach(print);println})
    val triangle = new Array[Array[Int]](10)
    for (i <- triangle.indices){
      triangle(i) = new Array[Int](i + 1)
    }
    triangle.foreach(tuple => {tuple.foreach(print);println})
    println()

    val command = ArrayBuffer("ls","-al","/home")
    val pb = new ProcessBuilder(JavaConversions.bufferAsJavaList(command))

    val cmd : mutable.Buffer[String] = JavaConversions.asScalaBuffer(pb.command())

    def randomArray1(n : Int) : Array[Int] = {
      val a = new Array[Int](n);
      for (i <- a.indices){
        a(i) = Random.nextInt(n)
      }

      a
    }

    println(randomArray1(10).mkString("<",",",">"))

    def replace2(array: Array[Int]) : Array[Int] ={

      for (i <- array.indices by 2 if i < array.length - 1){
        val temp = array(i)
        array(i) = array(i + 1)
        array(i + 1) = temp

      }

      array

    }

    println(replace2(Array(2,1,4,3,6)).mkString("<",",",">"))

    def replace3 (array: Array[Int]) : Array[Int] ={

      val newArr = for(i <- array.indices)yield {
        if (i % 2 == 0){
          if (i == array.length - 1){
            array(i)
          }else {
            array(i + 1)
          }
        }else{
          array(i - 1)
        }

      }

      newArr.toArray
    }
    println(replace3(Array(2,1,4,3,6)).mkString("<",",",">"))

    def sort4(array: Array[Int]) : Array[Int] ={

      val newArr = new Array[Int](array.length)
      var index = 0

      for (i <- array){
        if ( i > 0){
          newArr(index) = i
          index += 1
        }
      }

      for (i <- array){
        if ( i <= 0){
          newArr(index) = i
          index += 1
        }
      }

      newArr

    }

    println(sort4(Array(-2,1,4,0,6)).mkString("<",",",">"))

    def avg5(array: Array[Int]) : Double ={

      array.sum.toDouble / array.length.toDouble

    }

    println(avg5(Array(-2,1,4,0,6)))

    def reverse6(array: Array[Int]) : Array[Int] ={
      array.reverse
    }

    println(reverse6(Array(-2,1,4,0,6)).mkString("<",",",">"))

    def reverse6Buffer(arrayBuffer: ArrayBuffer[Int]) : ArrayBuffer[Int] ={
      arrayBuffer.reverse
    }

    println(reverse6Buffer(ArrayBuffer(-2,1,4,0,6)).mkString("<",",",">"))

    def distinct7(array: Array[Int]) : Array[Int] = {
      array.distinct
    }
    println(distinct7(Array(-2,1,0,0,6)).mkString("<",",",">"))

    def timeZone9() : Array[String] ={

      val timeZoneArr = java.util.TimeZone.getAvailableIDs()
      timeZoneArr.map(tuple => {
        if (tuple.startsWith("America/")){
          tuple.drop(8)
        }else{
          tuple
        }
      }).sorted
    }

    println(timeZone9().mkString("<",",",">"))
  }

  def four() : Unit ={

    val scores = Map(("A",1),"B" -> 2,"B" -> 2,"B" -> 3)
    println(scores.mkString("<",",",">") + "|" + scores.size)
    val scoresMutable = scala.collection.mutable.Map("A" -> 1,"B" -> 2,"B" -> 2,"B" -> 3)
    println(scoresMutable.mkString("<",",",">") + "|" + scores.size)
    val scoresNull = scala.collection.mutable.Map[String,Int]()

    println(scores("B"))
    try {
      println(scores("C"))
    }catch {
      case e : NoSuchElementException =>
    }

    println(if (scores.contains("C")) scores("C") else -1)
    println(scores.getOrElse("C",-1))
    scoresMutable("A") = 2
    scoresMutable += (("C",2))
    scoresMutable -= "D"
    val addScores : Map[String,Int] = scores + ("D" -> 2)
    val updateScores : Map[String,Int] = scores + ("A" -> 0)
    val removeScores : Map[String,Int] = scores - "A"

    for ((k,v) <- scores){
      print(k + "," + v)
    }
    println()

    val keys = scores.keySet
    val values = scores.valuesIterator

    println((for ((k,v) <- scores) yield (v,k)).mkString("<" ,",",">"))

    val scalaMap : mutable.Map[String,Int] = JavaConversions.mapAsScalaMap(new util.HashMap[String,Int]())

    val javaMap : java.util.Map[String,Int] = JavaConversions.mapAsJavaMap(Map[String,Int]())

    val tuple : Tuple3[String,Int,Int] = ("A",1,2)

    println(tuple._1 + "|" + tuple._2 + "|" + tuple._3)

    val name = Array("A","B","C")
    val job = ArrayBuffer("AA","BB","CC")
    val zip = name.zip(job)


    def practice1() : Unit = {

      val map = scala.collection.mutable.Map[String,Double]("A" -> 1,"B" -> 2,"C" -> 3)

      map.map(tuple => (tuple._1,tuple._2 * 0.9))
      println(map.mkString("<",",",">"))

      for ((k,v) <- map){
        map(k) = v * 0.9
      }

      println(map.mkString("<",",",">"))

    }
    practice1()

    def practice2() : Unit = {
      val in = new Scanner(new File("/Users/chenwei/Downloads/new 1.txt"))

      val map = new scala.collection.mutable.HashMap[String,Int]()

      while (in.hasNextLine){
        val line = in.next()
        line.split("\\.|'|$|:").foreach(tuple => {map(tuple) = map.getOrElse(tuple,1)})
      }
      println(map.mkString("<",",",">"))
    }

    practice2()

    def practice3() : Unit = {
      val in = new Scanner(new File("/Users/chenwei/Downloads/new 1.txt"))

      var map = Map[String,Int]()

      while (in.hasNextLine){
        val line = in.next()
        line.split("\\.|'|$|:").foreach(tuple => {map += (tuple -> map.getOrElse(tuple,1))})
      }
      println(map.mkString("<",",",">"))
    }

    practice3()

    def practice4() : Unit = {
      val in = new Scanner(new File("/Users/chenwei/Downloads/new 1.txt"))

      var map = scala.collection.immutable.SortedMap[String,Int]()

      while (in.hasNextLine){
        val line = in.next()
        line.split("\\.|'|$|:").foreach(tuple => {map += (tuple -> map.getOrElse(tuple,1))})
      }
      println(map.mkString("<",",",">"))
    }

    practice4()

    def practice5() : Unit = {
      val in = new Scanner(new File("/Users/chenwei/Downloads/new 1.txt"))

      var map = JavaConversions.mapAsScalaMap(new util.TreeMap[String,Int](new Comparator[String]() {
        override def compare(o1: String, o2: String) :Int ={
          o1.compareTo(o2)
        }
      }))

      while (in.hasNextLine){
        val line = in.next()
        line.split("\\.|'|$|:").foreach(tuple => {map += (tuple -> map.getOrElse(tuple,1))})
      }
      println(map.mkString("<",",",">"))
    }

    practice5()

    def practice6() : Unit = {
      var map = JavaConversions.mapAsScalaMap(new util.LinkedHashMap[String,Int])
      map += ("Monday"->Calendar.MONDAY)
      map += ("Tuesday"->Calendar.TUESDAY)
      map += ("Wednesday"->Calendar.WEDNESDAY)
      map += ("Thursday"->Calendar.THURSDAY)
      map += ("Friday"->Calendar.FRIDAY)
      map += ("Saturday"->Calendar.SATURDAY)
      map += ("Sunday"->Calendar.SUNDAY)


      println(map.mkString("<",",",">"))
    }

    practice6()

    def practice7() : Unit = {

      val map : scala.collection.Map[AnyRef,AnyRef]= JavaConversions.mapAsScalaMap(System.getProperties())
      println(map.mkString("<",",",">"))
      val keys = map.keySet

      var maxLength = -1
      for(key <- keys) {
        if (maxLength < key.toString.length){
          maxLength = key.toString.length
        }
      }
      for(key <- keys) {
        print(key)
        print(" " * (maxLength - key.toString.length))
        print(" | ")
        println(map(key))
      }

    }

    practice7()

    def practice8() : Unit = {

      val array = Array(1,5,2,7)

      def minmax(values:Array[Int]) : Tuple2[Int,Int] = {
        (values.max,values.min)
      }
      println(minmax(array))
    }

    practice8()

    def practice9() : Unit = {

      val array = Array(1,5,2,7)

      def iteqgt(values:Array[Int],v:Int) : Tuple3[Int,Int,Int] = {
        var it = 0
        var eq = 0
        var gt = 0

        for (value <- values){

          if (value < v){
            it += 1
          }else if(value == v){
            eq += 1
          }else{
            gt += 1
          }

        }

        (it,eq,gt)
      }
      println(iteqgt(array,5))
    }

    practice9()


    def practice10() : Unit = {
      val zip = "Hello".zip("World")

      zip.foreach(tuple => println(tuple._1 +"|" + tuple._2))

    }
    practice10()
  }

  def five() : Unit ={
    class Counter{
      private var value = 0

      def increment (){
        value += 1
      }

      def current() = value

      def prv = value - 1
    }


    val counter : Counter = new Counter;
    counter.increment()
    println(counter.current())

    counter.increment
    println(counter.current)

    println(counter.prv)


    class age{
      var age = 0
    }

    val age = new age
    age.age = 21
    println(age.age)

    class ageOnlyIncrement{
      private var privateAge = 0
      def age = privateAge
      def age_=(newValue:Int){
        if(newValue > privateAge) privateAge = newValue
      }
    }

    val ageOnlyIncrement = new ageOnlyIncrement
    ageOnlyIncrement.age = 21
    println(ageOnlyIncrement.age)
    ageOnlyIncrement.age = 20
    println(ageOnlyIncrement.age)


    class Counter2{
      private var value = 0

      def increment (){
        value += 1
      }
      def isLess(other: Counter2) = value < other.value
    }

    class Counter3{
      private[this] var value = 0

      def increment (){
        value += 1
      }
      //def isLess(other: Counter3) = value < other.value
    }

    class BeanTest{
      @BeanProperty var value = 0
    }

    val beanTest = new BeanTest
    beanTest.setValue(10)
    println(beanTest.getValue)

    class Person(var name : String = "infi", var age : Int = 18){

    }

    val person = new Person()
    println(person.name)

  }
}

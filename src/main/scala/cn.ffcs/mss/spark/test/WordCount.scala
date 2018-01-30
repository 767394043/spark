package cn.ffcs.mss.spark.test

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapred
import org.apache.hadoop.mapred.{JobConf, TextOutputFormat}
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partition, SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
/**
  * Created by Titanium on 2017/4/21.
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    operatorGroupByKeyTest()
  }

  def wordCount() : Unit = {
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val text : RDD[String] = sc.textFile("G:\\little talks.txt")
    val words : RDD[String] = text.flatMap( _.split(" "))
    val pairs : RDD[(String,Int)] = words.map((_,1))
    val results : RDD[(String,Int)] = pairs.reduceByKey(_+_)
    results.foreach(println)
  }

  def wordCountSortByWord() : Unit = {
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val text : RDD[String] = sc.textFile("G:\\little talks.txt")
    val words : RDD[String] = text.flatMap( _.split(" |'|!"))
    val pairs : RDD[(String,Int)] = words.map((_,1))
    val results : RDD[(String,Int)] = pairs.reduceByKey(_+_)
    val sorts = results.sortByKey()
    sorts.collect().foreach(println)
  }

  def wordCountSortByNumber() : Unit = {
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val text : RDD[String] = sc.textFile("G:\\little talks.txt")
    val words : RDD[String] = text.flatMap( _.split(" |'|!"))
    val pairs : RDD[(String,Int)] = words.map((_,1))
    val results : RDD[(String,Int)] = pairs.reduceByKey(_+_)
    val valueKey : RDD[(Int,String)] = results.map( tuple => (tuple._2,tuple._1))
    val sorts = valueKey.sortByKey(false)
    val keyValue : RDD[(String,Int)] = sorts.map( tuple => (tuple._2, tuple._1))
    keyValue.collect().foreach(println)

    sorts.persist()
  }

  def operatorMap() : Unit ={
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val text : RDD[String] = sc.textFile("G:\\little talks2.txt")
    val words : RDD[String] = text.flatMap( _.split(" |'|!"))
    val maps : RDD[(String,Int)] = words.map(tuple => (tuple,tuple.length))
    maps.collect().foreach(println)
  }

  def operatorFilter() : Unit ={
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val text : RDD[String] = sc.textFile("G:\\little talks.txt")
    val words : RDD[String] = text.flatMap( _.split(" |'|!"))
    val maps : RDD[String] = words.filter(tuple => (tuple.length > 4))
    maps.collect().foreach(println)
  }

  def operatorFlatMap() : Unit ={
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val text : RDD[String] = sc.textFile("G:\\little talks.txt")
    val words : RDD[(String,Int)] = text.flatMap(tuple => flatMapFunction(tuple))
    words.collect().foreach(println)
  }

  def flatMapFunction(string: String) : ArrayBuffer[(String,Int)] = {

    val arrayBuffer : ArrayBuffer[(String,Int)] = new ArrayBuffer[(String,Int)]()
    val words : Array[String] = string.split(" |'|!")
    for(string <- words){
      arrayBuffer += (elem = (string,string.length))
    }

    return arrayBuffer
  }

  def operatorSample() : Unit ={
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val text : RDD[String] = sc.textFile("G:\\little talks.txt")
    val words : RDD[String] = text.flatMap( _.split(" |'|!"))
    val maps : RDD[(String,Int)] = words.map(tuple => (tuple,tuple.length))
    //false表示不放回的抽取
    //true表示有放回的抽取
    //Double类型的参数表示抽取的百分比
    //Long类型的参数表示随机种子
    val sample : RDD[(String,Int)]= maps.sample(false,0.01,100)

    sample.collect().foreach(println)
    println(sample.count())
    println(maps.count())
  }

  def operatorGroupByKey() : Unit ={
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val text : RDD[String] = sc.textFile("G:\\little talks.txt")
    val words : RDD[String] = text.flatMap( _.split(" |'|!"))
    val maps : RDD[(String,Int)] = words.map(tuple => (tuple,tuple.length))
    val groupByKey : RDD[(String,Iterable[Int])] = maps.groupByKey()
//    groupByKey.foreach(println)
    groupByKey.foreach(tuple => tuple._2.foreach(x => println("(" + tuple._1 + "," + x + ")")))
    val flatMap : RDD[(String,Int)] = groupByKey.flatMapValues(tuple => tuple.toList)
//    flatMap.foreach(println)
  }

  def operatorReduceByKey() : Unit ={
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val text : RDD[String] = sc.textFile("G:\\little talks.txt")
    val words : RDD[String] = text.flatMap( _.split(" |'|!"))
    val map : RDD[(String,Int)] = words.map(tuple => (tuple,1))
    val reduceByKey : RDD[(String,Int)] = map.reduceByKey((_+_))
    reduceByKey.collect().foreach(println)
  }

  def operatorUnion() : Unit ={
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val text_1 : RDD[String] = sc.textFile("G:\\little talks.txt")
    val text_2 : RDD[String] = sc.textFile("G:\\Turnin'.txt")
    val words_1 : RDD[String] = text_1.flatMap( _.split(" |'|!"))
    val words_2 : RDD[String] = text_2.flatMap( _.split(" |'|!|(|)"))
    val map_1 : RDD[(String,Int)] = words_1.map(tuple => (tuple,1))
    val map_2 : RDD[(String,Int)] = words_2.map(tuple => (tuple,1))
    val union : RDD[(String,Int)] = map_1.union(map_2)
    val reduceByKey : RDD[(String,Int)] = union.reduceByKey((_+_))
    reduceByKey.collect().foreach(println)
  }

  def operatorJoin() : Unit ={
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val text_1 : RDD[String] = sc.textFile("G:\\little talks.txt")
    val text_2 : RDD[String] = sc.textFile("G:\\Turnin'.txt")
    val words_1 : RDD[String] = text_1.flatMap( _.split(" |'|!"))
    val words_2 : RDD[String] = text_2.flatMap( _.split(" |'|!|(|)"))
    val map_1 : RDD[(String,Int)] = words_1.map(tuple => (tuple,1))
    val map_2 : RDD[(String,Int)] = words_2.map(tuple => (tuple,1))
    val reduceByKey_1 : RDD[(String,Int)] = map_1.reduceByKey((_+_))
    val reduceByKey_2 : RDD[(String,Int)] = map_2.reduceByKey((_+_))
    val join : RDD[(String,(Int,Int))] = reduceByKey_1.join(reduceByKey_2)
    join.collect().foreach(println)
  }

  def operatorCogroup() : Unit ={
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val text_1 : RDD[String] = sc.textFile("G:\\little talks.txt")
    val text_2 : RDD[String] = sc.textFile("G:\\Turnin'.txt")
    val words_1 : RDD[String] = text_1.flatMap( _.split(" |'|!"))
    val words_2 : RDD[String] = text_2.flatMap( _.split(" |'|!|(|)"))
    val map_1 : RDD[(String,Int)] = words_1.map(tuple => (tuple,1))
    val map_2 : RDD[(String,Int)] = words_2.map(tuple => (tuple,1))
    val reduceByKey_1 : RDD[(String,Int)] = map_1.reduceByKey((_+_))
    val reduceByKey_2 : RDD[(String,Int)] = map_2.reduceByKey((_+_))
    val cogroup : RDD[(String,(Iterable[Int],Iterable[Int]))] = reduceByKey_1.cogroup(reduceByKey_2)
    cogroup.collect().foreach(println)
  }

  def operatorCrossProduct() : Unit ={

  }

  def operatorMapValue() : Unit ={
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val text : RDD[String] = sc.textFile("G:\\little talks.txt")
    val words : RDD[String] = text.flatMap( _.split(" |'|!"))
    val map : RDD[(String,Int)] = words.map(tuple => (tuple,1))
    val mapValue : RDD[(String,Long)] = map.mapValues((_.toLong))
    val reduceByKey : RDD[(String,Long)] = mapValue.reduceByKey((_+_))
    reduceByKey.collect().foreach(println)
  }

  def operatorSortByKey() : Unit ={
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val text : RDD[String] = sc.textFile("G:\\little talks.txt")
    val words : RDD[String] = text.flatMap( _.split(" |'|!"))
    val map : RDD[(String,Int)] = words.map(tuple => (tuple,1))
    val mapValue : RDD[(String,Long)] = map.mapValues((_.toLong))
    val reduceByKey : RDD[(String,Long)] = mapValue.reduceByKey((_+_))
    val sort : RDD[(String,Long)] = reduceByKey.sortByKey(true)
    sort.collect().foreach(println)
  }

  def operatorPartitionBy() : Unit ={

  }

  def operatorCount() : Unit ={
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val text : RDD[String] = sc.textFile("G:\\little talks.txt")
    val words : RDD[String] = text.flatMap( _.split(" |'|!"))
    val map : RDD[(String,Int)] = words.map(tuple => (tuple,1))
    val count : Long = map.count()
    println(count)
  }

  def operatorCollect() : Unit ={
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val text : RDD[String] = sc.textFile("G:\\little talks.txt")
    val words : RDD[String] = text.flatMap( _.split(" |'|!"))
    val map : RDD[(String,Int)] = words.map(tuple => (tuple,1))
    val collect : Array[(String,Int)] = map.collect()
    collect.foreach(println)
  }

  def operatorReduce() : Unit ={
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val text : RDD[String] = sc.textFile("G:\\little talks.txt")
    val words : RDD[String] = text.flatMap( _.split(" |'|!"))
    val map : RDD[(String,Int)] = words.map(tuple => (tuple,1))
    val collect : (String,Int) = map.reduce((A,B) => (A._1 + B._1, A._2 + B._2))
    println(collect._1)
    println(collect._2)
  }

  def operatorLookup() : Unit ={
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val text : RDD[String] = sc.textFile("G:\\little talks.txt")
    val words : RDD[String] = text.flatMap( _.split(" |'|!"))
    val map : RDD[(String,Int)] = words.map(tuple => (tuple,1))
    val collect : Seq[Int] = map.lookup("Hey")
    collect.foreach(println)
  }

  def operatorSaveAsTextFile() : Unit ={
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val text : RDD[String] = sc.textFile("G:\\little talks.txt")
    val words : RDD[String] = text.flatMap( _.split(" |'|!"))
    val map : RDD[(String,Int)] = words.map(tuple => (tuple,1))
    map.saveAsTextFile("G:\\little talks2.txt")
  }

  def operatorMapPartitions() : Unit ={
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val text : RDD[String] = sc.textFile("G:\\little talks.txt")
    val words : RDD[String] = text.flatMap( _.split(" |'|!"))
    val partition : RDD[String] = words.coalesce(5,false)
    val mapPartitions : RDD[(String,Int)]= partition.mapPartitions(iterator => {
      var result = List[(String,Int)]()
      while (iterator.hasNext){
        val temp : String = iterator.next()
        result ::= (temp,temp.length)
      }
      result.iterator
    },true)

    mapPartitions.foreach(println)
  }


  def operatorGlom() : Unit ={
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val text : RDD[String] = sc.textFile("G:\\little talks.txt")
    val words : RDD[String] = text.flatMap( _.split(" |'|!"))
    val map : RDD[(String,Int)] = words.map(tuple => (tuple,1))
    val glom : RDD[Array[(String,Int)]] = map.glom()
    glom.collect().foreach(_.foreach(println))
  }

  def operatorCartesian() : Unit ={
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val text_1 : RDD[String] = sc.textFile("G:\\little talks.txt")
    val text_2 : RDD[String] = sc.textFile("G:\\Turnin'.txt")
    val words_1 : RDD[String] = text_1.flatMap( _.split(" |'|!"))
    val words_2 : RDD[String] = text_2.flatMap( _.split(" |'|!|(|)"))
    val map_1 : RDD[(String,Int)] = words_1.map(tuple => (tuple,1))
    val map_2 : RDD[(String,Int)] = words_2.map(tuple => (tuple,1))
    val reduceByKey_1 : RDD[(String,Int)] = map_1.reduceByKey((_+_))
    val reduceByKey_2 : RDD[(String,Int)] = map_2.reduceByKey((_+_))
    val cartesian : RDD[((String,Int),(String,Int))] = reduceByKey_1.cartesian(reduceByKey_2)
    cartesian.collect().foreach(println)

    println("reduceByKey_1.count()=" + reduceByKey_1.count() + " " )
    println("reduceByKey_2.count()=" + reduceByKey_2.count() + " " )
    println("cartesian.count()=" + cartesian.count() + " " )
  }

  def operatorGroupBy() : Unit ={
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val text : RDD[String] = sc.textFile("G:\\little talks.txt")
    val words : RDD[String] = text.flatMap( _.split(" |'|!"))
    val map : RDD[(String,Int)] = words.map(tuple => (tuple,1))
    val groupBy : RDD[(String, Iterable[(String,Int)])]= map.groupBy(tuple => {if(tuple._1.length < 4) "short" else "long"})
    groupBy.foreach(tuple => ({
      print(tuple._1 + ":")
      tuple._2.foreach( x => (
        print(x+",")))
      println()
    }))
  }

  def operatorDistinct() : Unit ={
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val text : RDD[String] = sc.textFile("G:\\little talks.txt")
    val words : RDD[String] = text.flatMap( _.split(" |'|!"))
    val map : RDD[(String,Int)] = words.map(tuple => (tuple,1))
    val distinct : RDD[(String,Int)] = map.distinct()
    distinct.collect().foreach(println)
  }

  def operatorIntersection() : Unit ={
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val text_1 : RDD[String] = sc.textFile("G:\\little talks.txt")
    val text_2 : RDD[String] = sc.textFile("G:\\Turnin'.txt")
    val words_1 : RDD[String] = text_1.flatMap( _.split(" |'|!"))
    val words_2 : RDD[String] = text_2.flatMap( _.split(" |'|!|(|)"))
    val map_1 : RDD[(String,Int)] = words_1.map(tuple => (tuple,1))
    val map_2 : RDD[(String,Int)] = words_2.map(tuple => (tuple,1))
    val intersection : RDD[(String,Int)] = map_1.intersection(map_2)
    intersection.collect().foreach(println)
  }

  def operatorSubtract() : Unit ={
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val text_1 : RDD[String] = sc.textFile("G:\\little talks.txt")
    val text_2 : RDD[String] = sc.textFile("G:\\Turnin'.txt")
    val words_1 : RDD[String] = text_1.flatMap( _.split(" |'|!"))
    val words_2 : RDD[String] = text_2.flatMap( _.split(" |'|!|(|)"))
    val map_1 : RDD[(String,Int)] = words_1.map(tuple => (tuple,1))
    val map_2 : RDD[(String,Int)] = words_2.map(tuple => (tuple,1))
    val subtract : RDD[(String,Int)] = map_1.subtract(map_2)
    subtract.collect().foreach(println)
  }

  def operatorSortBy() : Unit ={
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val text : RDD[String] = sc.textFile("G:\\little talks.txt")
    val words : RDD[String] = text.flatMap( _.split(" |'|!"))
    val map : RDD[(String,Int)] = words.map(tuple => (tuple,tuple.length))
    //第一个参数是要排序的值
    //第二个参数为true是降序，false是升序
    //第三个参数是排序之后分区个数
    val sortBy : RDD[(String,Int)] = map.sortBy(x => x._2,true)
    sortBy.collect().foreach(println)
  }

  def operatorTakeSample() : Unit ={
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val text : RDD[String] = sc.textFile("G:\\little talks.txt")
    val words : RDD[String] = text.flatMap( _.split(" |'|!"))
    val map : RDD[(String,Int)] = words.map(tuple => (tuple,tuple.length))
    //第一个参数为true是有放回的抽取，false是无放回的抽取
    //第二个参数为抽取的个数
    //第三个参数是表示用于指定的随机数生成器种子
    val takeSample : Array[(String,Int)] = map.takeSample(false,2,100)
    takeSample.foreach(println)
  }

  def operatorTakeOrder() : Unit ={
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val text : RDD[String] = sc.textFile("G:\\little talks.txt")
    val words : RDD[String] = text.flatMap( _.split(" |'|!"))
    val map : RDD[(String,Int)] = words.map(tuple => (tuple,tuple.length))
    val takeSample : Array[(String,Int)]= map.takeOrdered(2)
    takeSample.foreach(println)
  }

  def operatorCombineByKey() : Unit ={
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val text : RDD[String] = sc.textFile("G:\\little talks.txt")
    val words : RDD[String] = text.flatMap( _.split(" |'|!"))
    val map : RDD[(String,Int)] = words.map(tuple => (tuple,tuple.length))
    //主要接受了三个函数作为参数，分别为createCombiner、mergeValue、mergeCombiners
    //createCombiner:组合器函数，用于将V类型转换成C类型，输入参数为RDD[K,V]中的V,输出为C
    //mergeValue:合并值函数，将一个C类型和一个V类型值合并成一个C类型，输入参数为(C,V)，输出为C
    //mergeCombiners:合并组合器函数，用于将两个C类型值合并成一个C类型，输入参数为(C,C)，输出为C
    //numPartitions：结果RDD分区数，默认保持原有的分区数
    //partitioner：分区函数,默认为HashPartitioner
    //mapSideCombine：是否需要在Map端进行combine操作，类似于MapReduce中的combine，默认为true
    val combineByKey : RDD[(String,(Int,Int))]= map.combineByKey(
      (v : (Int)) => (v,1),
      (acc : (Int,Int),v : Int) => (acc._1 + v, acc._2 + 1),
      (acc1 : (Int,Int), acc2 : (Int,Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
    val mapValues : RDD[(String,Double)] = combineByKey.mapValues(tuple => (tuple._1 /tuple._2).toDouble)
    mapValues.collect().foreach(println)
  }

  def operatorFoldByKey() : Unit ={
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val text : RDD[String] = sc.textFile("G:\\little talks.txt")
    val words : RDD[String] = text.flatMap( _.split(" |'|!"))
    val map : RDD[(String,Int)] = words.map(tuple => (tuple,1))
    val foldByKey : RDD[(String,Int)]= map.foldByKey(0)(strcat(_)(_)(2))
    foldByKey.sortBy(x => x._2,true).collect().foreach(println)
  }

  def strcat(s1: Int)(s2: Int)(s3 : Int) = (s1 + s2) * s3

  def operatorLeftOuterJoin() : Unit ={
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val text_1 : RDD[String] = sc.textFile("G:\\little talks.txt")
    val text_2 : RDD[String] = sc.textFile("G:\\Turnin'.txt")
    val words_1 : RDD[String] = text_1.flatMap( _.split(" |'|!"))
    val words_2 : RDD[String] = text_2.flatMap( _.split(" |'|!|(|)"))
    val map_1 : RDD[(String,Int)] = words_1.map(tuple => (tuple,1))
    val map_2 : RDD[(String,Int)] = words_2.map(tuple => (tuple,1))
    val leftOuterJoin : RDD[(String,(Int,Option[Int]))] = map_1.leftOuterJoin(map_2)
    leftOuterJoin.collect().foreach(tuple => {
      print(tuple._1 +":" + tuple._2._1 + ",")
      tuple._2._2.foreach(x => print(x))
      println()})
  }

  def operatorRightOuterJoin() : Unit ={
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val text_1 : RDD[String] = sc.textFile("G:\\little talks.txt")
    val text_2 : RDD[String] = sc.textFile("G:\\Turnin'.txt")
    val words_1 : RDD[String] = text_1.flatMap( _.split(" |'|!"))
    val words_2 : RDD[String] = text_2.flatMap( _.split(" |'|!|\\(|\\)"))
    val map_1 : RDD[(String,Int)] = words_1.map(tuple => (tuple,1))
    val map_2 : RDD[(String,Int)] = words_2.map(tuple => (tuple,1))
    val rightOuterJoin : RDD[(String,(Option[Int],Int))] = map_1.rightOuterJoin(map_2)
    rightOuterJoin.collect().foreach(tuple => {
      print(tuple._1 +":")
      tuple._2._1.foreach(x => print(x))
      print(","+tuple._2._2)
      println()})
  }

  def operatorForeach() : Unit ={
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val text : RDD[String] = sc.textFile("G:\\little talks.txt")
    val words : RDD[String] = text.flatMap( _.split(" |'|!"))
    val map : RDD[(String,Int)] = words.map(tuple => (tuple,1))
    map.foreach(println)
  }

  def operatorForeachPartition() : Unit ={
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val text : RDD[String] = sc.textFile("G:\\little talks.txt")
    val words : RDD[String] = text.flatMap( _.split(" |'|!"))
    val map : RDD[(String,Int)] = words.map(tuple => (tuple,1))
    map.foreachPartition( tuple => (tuple.foreach(x => println(x))))
  }

  def operatorSaveAsSequenceFile() : Unit ={
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val text : RDD[String] = sc.textFile("G:\\little talks.txt")
    val words : RDD[String] = text.flatMap( _.split(" |'|!"))
    val map : RDD[(String,Int)] = words.map(tuple => (tuple,1))
    map.saveAsSequenceFile("G:\\little talks2.txt")
  }

  def operatorSaveAsObjectFile() : Unit ={
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val text : RDD[String] = sc.textFile("G:\\little talks.txt")
    val words : RDD[String] = text.flatMap( _.split(" |'|!"))
    val map : RDD[(String,Int)] = words.map(tuple => (tuple,1))
    map.saveAsObjectFile("G:\\little talks2.txt")
  }

  def operatorCollectAsMap() : Unit ={
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val text : RDD[String] = sc.textFile("G:\\little talks.txt")
    val words : RDD[String] = text.flatMap( _.split(" |'|!"))
    val map : RDD[(String,Int)] = words.map(tuple => (tuple,1))
    val partition : RDD[(String,Int)] = map.partitionBy(new HashPartitioner(2))
    val collectAsMap : scala.collection.Map[String,Int] = partition.collectAsMap()
    for(temp <- collectAsMap){
      println(temp._1 + "," + temp._2)
    }
  }

  def operatorFlatMapValues() : Unit ={
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val text : RDD[String] = sc.textFile("G:\\little talks.txt")
    val words : RDD[String] = text.flatMap( _.split(" |'|!"))
    val map : RDD[(String,Int)] = words.map(tuple => (tuple,tuple.length))
    val flatMapValues : RDD[(String,Int)]= map.flatMapValues( x => 1 to x)
    flatMapValues.foreach(println)
  }

  def operatorReduceByKeyLocally() : Unit ={
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val text : RDD[String] = sc.textFile("G:\\little talks.txt")
    val words : RDD[String] = text.flatMap( _.split(" |'|!"))
    val map : RDD[(String,Int)] = words.map(tuple => (tuple,1))
    val reduceByKeyLocally : scala.collection.Map[String,Int] = map.reduceByKeyLocally((tuple1,tuple2) => (tuple1 + tuple2))
    for(temp <- reduceByKeyLocally){
      println(temp._1 + "," + temp._2)
    }
  }

  def operatorTop() : Unit ={
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val text : RDD[String] = sc.textFile("G:\\little talks.txt")
    val words : RDD[String] = text.flatMap( _.split(" |'|!"))
    val map : RDD[(String,Int)] = words.map(tuple => (tuple,1))
    implicit val myOrd = implicitly[Ordering[String]].reverse
    val top : Array[(String,Int)] = map.top(3)
    for(temp <- top){
      println(temp._1 + "," + temp._2)
    }
  }

  def operatorTakeOrdered() : Unit ={
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val text : RDD[String] = sc.textFile("G:\\little talks.txt")
    val words : RDD[String] = text.flatMap( _.split(" |'|!"))
    val map : RDD[(String,Int)] = words.map(tuple => (tuple,1))
    val takeOrdered : Array[(String,Int)] = map.takeOrdered(3)
    for(temp <- takeOrdered){
      println(temp._1 + "," + temp._2)
    }
  }

  def operatorFirst() : Unit ={
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val text : RDD[String] = sc.textFile("G:\\little talks.txt")
    val words : RDD[String] = text.flatMap( _.split(" |'|!"))
    val map : RDD[(String,Int)] = words.map(tuple => (tuple,1))
    val first : (String,Int) = map.first()
    println(first._1 + " " + first._2)
  }

  def operatorAggregate() : Unit ={
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val text : RDD[String] = sc.textFile("G:\\little talks.txt")
    val words : RDD[String] = text.flatMap( _.split(" |'|!"))
    val partition : RDD[String] = words.coalesce(5,false)
    val map : RDD[(String,Int)] = partition.map(tuple => (tuple,1))
    val aggregate : Int = map.aggregate(0)((x,string) => (1 + x),(x1,x2) => (x1 + x2))
    println(aggregate)
  }

  def operatorRandomSplit() : Unit ={
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val text : RDD[String] = sc.textFile("G:\\little talks.txt")
    val words : RDD[String] = text.flatMap( _.split(" |'|!"))
    val map : RDD[(String,Int)] = words.map(tuple => (tuple,1))
    val randomSplit : Array[RDD[(String,Int)]] = map.randomSplit(Array(1,2,3,4),100)

    println(randomSplit(0).count() + " " + 1.0 * randomSplit(0).count()/map.count() + " " + 1.0 * 1/10)
    println(randomSplit(1).count() + " " + 1.0 * randomSplit(1).count()/map.count() + " " + 1.0 * 2/10)
    println(randomSplit(2).count() + " " + 1.0 * randomSplit(2).count()/map.count() + " " + 1.0 * 3/10)
    println(randomSplit(3).count() + " " + 1.0 * randomSplit(3).count()/map.count() + " " + 1.0 * 4/10)
  }

  def operatorCoalesce() : Unit ={
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val text : RDD[String] = sc.textFile("G:\\little talks.txt")
    val words : RDD[String] = text.flatMap( _.split(" |'|!"))
    val map : RDD[(String,Int)] = words.map(tuple => (tuple,1))
    val coalesce : RDD[(String,Int)] = map.coalesce(2,true)
  }

  def operatorRepartition() : Unit ={
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val text : RDD[String] = sc.textFile("G:\\little talks.txt")
    val words : RDD[String] = text.flatMap( _.split(" |'|!"))
    val map : RDD[(String,Int)] = words.map(tuple => (tuple,1))
    val repartition : RDD[(String,Int)] = map.repartition(2)
  }

  def operatorZip() : Unit ={
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val rdd1 : RDD[Int] = sc.parallelize(Array(1,2,3,4,5))
    val rdd2 : RDD[String] = sc.parallelize(Array("A","B","C","D","E"))
    val zip12 : RDD[(Int,String)] = rdd1.zip(rdd2)
    val zip21 : RDD[(String,Int)] = rdd2.zip(rdd1)

    zip12.foreach(println)
    println()
    zip21.foreach(println)
  }

  def operatorZipPartitions_1() : Unit ={
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val rdd1 : RDD[Int] = sc.parallelize(Array(1,2,3,4,5,6))
    val rdd2 : RDD[String] = sc.parallelize(Array("A","B","C","D","E","F"))
    val partition_1 : RDD[Int] = rdd1.coalesce(2,false)
    val partition_2 : RDD[String] = rdd2.coalesce(2,false)

    val zipPartitions12 : RDD[(Int,String)] = partition_1.zipPartitions(partition_2){
      (iterator1 : Iterator[Int], iterator2 : Iterator[String]) =>{
        var result = List[(Int,String)]()
        while (iterator1.hasNext && iterator2.hasNext){
          result ::= (iterator1.next(),iterator2.next())
        }
        result.iterator
      }
    }
    zipPartitions12.foreach(println)

    val zipPartitions21 : RDD[(String,Int)] = partition_2.zipPartitions(partition_1){
      (iterator1 : Iterator[String], iterator2 : Iterator[Int]) =>{
        var result = List[(String,Int)]()
        while (iterator1.hasNext && iterator2.hasNext){
          result ::= (iterator1.next(),iterator2.next())
        }
        result.iterator
      }
    }
    zipPartitions21.foreach(println)
  }

  def operatorZipWithIndex() : Unit ={
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val rdd : RDD[String] = sc.parallelize(Array("A","B","C","D","E","F"))
    val zipWithIndex : RDD[(String,Long)] = rdd.zipWithIndex()
    zipWithIndex.foreach(println)
  }

  def operatorZipWithUniqueId() : Unit ={
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val rdd : RDD[String] = sc.parallelize(Array("A","B","C","D","E","F"))
    val partition : RDD[String] = rdd.coalesce(3)
    val zipWithIndex : RDD[(String,Long)] = partition.zipWithUniqueId()
    zipWithIndex.foreach(println)
  }

  def operatorSaveAsHadoopFile() : Unit ={
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val rdd : RDD[String] = sc.parallelize(Array("A","B","C","D","E","F"))
    val map : RDD[(String,Int)] = rdd.map(tuple => (tuple, tuple.length))
    map.saveAsHadoopFile("hdfs:\\is-nn-01:8020\result",classOf[Text],classOf[IntWritable],classOf[TextOutputFormat[Text,IntWritable]])
  }

  def operatorSaveAsHadoopDataSet() : Unit ={

    val conf = new SparkConf().setAppName("wordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val rdd : RDD[String] = sc.parallelize(Array("A","B","C","D","E","F"))
    val map : RDD[(String,Int)] = rdd.map(tuple => (tuple, tuple.length))

    val jobConf : JobConf = new mapred.JobConf()
    jobConf.setOutputFormat(classOf[TextOutputFormat[Text,IntWritable]])
    jobConf.setOutputKeyClass(classOf[Text])
    jobConf.setOutputValueClass(classOf[IntWritable])
    jobConf.set("mapred.output.dir","hdfs:\\is-nn-01:8020\result")
    map.saveAsHadoopDataset(jobConf)
  }

  def operatorSaveAsNewAPIHadoopFile() : Unit ={
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val rdd : RDD[String] = sc.parallelize(Array("A","B","C","D","E","F"))
    val map : RDD[(String,Int)] = rdd.map(tuple => (tuple, tuple.length))
    map.saveAsNewAPIHadoopFile("hdfs:\\is-nn-01:8020\result")
  }

  def operatorSaveAsNewAPIHadoopDataSet() : Unit ={

    val conf = new SparkConf().setAppName("wordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val rdd : RDD[String] = sc.parallelize(Array("A","B","C","D","E","F"))
    val map : RDD[(String,Int)] = rdd.map(tuple => (tuple, tuple.length))

    val jobConf : JobConf = new mapred.JobConf()
    jobConf.setOutputFormat(classOf[TextOutputFormat[Text,IntWritable]])
    jobConf.setOutputKeyClass(classOf[Text])
    jobConf.setOutputValueClass(classOf[IntWritable])
    jobConf.set("mapred.output.dir","hdfs:\\is-nn-01:8020\result")
    map.saveAsNewAPIHadoopDataset(jobConf)
  }

//  def operatorMapWith() : Unit ={
//    val conf = new SparkConf().setAppName("wordCount").setMaster("local[2]")
//    val sc = new SparkContext(conf)
//
//    val text : RDD[String] = sc.textFile("G:\\little talks.txt")
//    val words : RDD[String] = text.flatMap( _.split(" |'|!"))
//    val partition : RDD[String] = words.coalesce(3,true)
//    val map : RDD[((String,Int),Int)] = partition.mapWith(x => (x))((string,index) => ((string,string.length),index+1))
//    val mapValue : RDD[((String,Int),String)]= map.mapValues(tuple => (new String("partitionIndex:" + tuple)))
//    mapValue.foreach(println)
//  }
//
//  def operatorFlatMapWith() : Unit ={
//    val conf = new SparkConf().setAppName("wordCount").setMaster("local[2]")
//    val sc = new SparkContext(conf)
//
//    val text : RDD[String] = sc.textFile("G:\\little talks.txt")
//    val words : RDD[String] = text.flatMap( _.split(" |'|!"))
//    val partition : RDD[String] = words.coalesce(3,true)
//    val map : RDD[((String,Int),Int)] = partition.flatMapWith(x => (x))((string,index) => (flatMapWithFunction(string,index)))
//    val mapValue : RDD[((String,Int),String)]= map.mapValues(tuple => (new String("partitionIndex:" + tuple)))
//    mapValue.foreach(println)
//  }

  def flatMapWithFunction(string : String,x : Int) : ArrayBuffer[((String,Int),Int)] = {

    val arrayBuffer : ArrayBuffer[((String,Int),Int)] = new ArrayBuffer[((String,Int),Int)]()
    val words : Array[String] = string.split(" |'|!")
    for(string <- words){
      arrayBuffer += (elem = ((string,string.length),x+1))
    }

    return arrayBuffer
  }

//  def operatorMapWithTest() : Unit ={
//    val conf = new SparkConf().setAppName("wordCount").setMaster("local[2]")
//    val sc = new SparkContext(conf)
//
//    val text : RDD[String] = sc.textFile("G:\\little talks.txt")
//    val words : RDD[String] = text.flatMap( _.split(" |'|!"))
//    val partition : RDD[String] = words.coalesce(3,true)
//    val map : RDD[String] = partition.mapWith(x => (x))((string,index) => (string))
//    val mapMap : RDD[((String,Int),Int)] = map.mapWith(x => (x))((string,index) => ((string,string.length),index+1))
//    val mapValue : RDD[((String,Int),String)]= mapMap.mapValues(tuple => (new String("partitionIndex:" + tuple)))
//    mapValue.foreach(println)
//  }

  def operatorGroupByKeyTest() : Unit ={
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val text : RDD[String] = sc.textFile("/Users/chenwei/Downloads/ProjectHomePageMapper.xml")
    val words : RDD[String] = text.flatMap( _.split(" |'|!"))
    val maps : RDD[(String,Int)] = words.map(tuple => (tuple,tuple.length))
    val partition : RDD[(String,Int)] = maps.coalesce(5,false)
    val groupByKey : RDD[(String,Iterable[Int])] = partition.groupByKey()
    groupByKey.foreach(tuple => tuple._2.foreach(x => println("(" + tuple._1 + "," + x + ")")))
    val partitions : Array[Partition]= groupByKey.partitions

    for(temp <- partitions){
      
    }
  }
}



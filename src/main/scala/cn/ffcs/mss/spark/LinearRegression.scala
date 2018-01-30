package cn.ffcs.mss.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object LinearRegression {

  def main(args: Array[String]): Unit = {

    val conf :SparkConf = new SparkConf().setAppName("wordCount").setMaster("local[2]")
    val sc :SparkContext = new SparkContext(conf)
    val text :RDD[String]= sc.textFile("/Users/chenwei/Downloads/sparkTest.txt")
    val value : RDD[(Int,Int)] = text.map(tuple => (tuple.split(",")(0).toInt,tuple.split(",")(1).toInt))

    value.foreach(println)


  }

}

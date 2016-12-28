/**
  * Created by wuyiran on 12/26/16.
  */
package com.iwantfind

import org.apache.hadoop.conf._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._

object DataFramesAnalyser {

  def main(args: Array[String]) {
    val path = "/home/YiRan/workspace/iwantfind-tool/new2.json";

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
       .master("local")
      .getOrCreate()

    val df = spark.read.json(path)
    df.show()
    df.printSchema()

    import spark.implicits._

    //1 统计关注别人最多的10个用户，并计算关注最多的人与最少的人相差多少
    val  max = df.select("id", "total_number").orderBy( - df("total_number")).take(1).apply(0)
    val  min = df.select("id", "total_number").orderBy( df("total_number")).take(1).apply(0)
    var m1 = max.getLong(1)
    var m2 = min.getLong(1)
    var sub = m1 - m2

    //2 统计目前数据中被关注最多的10个用户（需要去重）
    var tt = df.withColumn("ids",explode(col("ids")))
    var t2 = tt.select("ids").map(row => (row.getLong(0), 1)).rdd.reduceByKey(_+_).sortBy(_._2, false).take(10)


    //3  统计这个数据集中有多少用户
    var a1 = tt.select("ids").distinct().count()
    var a2 = tt.select("id").distinct().count()
    var usernum = a1 + a2
    println("关注别人最多用户 =" + max.get(0) + " -> " + max.get(1) +", 关注别人最少用户 = " + min.get(0) + " -> " + min.get(1) + ", 相差 = " + sub  )
    t2.foreach(println)
    println("s总用户数：" + usernum)
  }
}
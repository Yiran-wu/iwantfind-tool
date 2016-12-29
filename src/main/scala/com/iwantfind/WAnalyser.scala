/**
  * Created by wuyiran on 12/26/16.
  */
package com.iwantfind

import org.apache.hadoop.conf._
import org.apache.spark.{SparkConf, SparkContext}
import play.api.libs.json._

import scala.collection.mutable.ArrayBuffer

case class UserInfo (id: String, ids : Array[String], next_cursor : String, previous_cursor: String, total_number : String)

object WAnalyser {

  def parseJSON(jsonStr : String) : UserInfo = {
    val json = Json.parse(jsonStr)
    val idsstr = json.\("ids").toString()
    var ids = new Array[String](0)
    if (idsstr.length > 1) {
      ids = idsstr.substring(1, idsstr.length() - 1).split(',')
    }
    UserInfo(json.\("id").toString(),
      ids,
      json.\("next_cursor").toString(),
      json.\("previous_cursor").toString(),
      json.\("total_number").toString())

  }

  def process (path: String) = {

    // 1. 创建sparkcontext
    val sc = new SparkContext ("local", "test", new SparkConf())
    //val sc = new SparkContext ( new SparkConf().setAppName("weibo test"))
    val fileSet = sc.textFile(path)

    // 2. 统计关注别人最多的10个用户，并计算关注最多的人与最少的人相差多少
    val vdata = fileSet.map( line => {
        val json = parseJSON(line)
        (json.id, json.total_number.toInt)
    }).sortBy(_._2, true)
    val min = vdata.take(1);
    val max = vdata.takeOrdered(1)(Ordering[Int].reverse.on(x => x._2))
    val sub = max.apply(0)._2 - min.apply(0)._2

    // 3.统计目前数据中被关注最多的10个用户（需要去重）
    val topUser = fileSet.flatMap(line => {
      val json = parseJSON(line)
      val array = new ArrayBuffer[String]()
      json.ids.copyToBuffer(array)
      array += json.id
      array
    }).map((_,1 )).reduceByKey(_+_).sortBy(_._2, false)
    val top10 = topUser.take(10)

    //4. 统计这个数据集中有多少用户
    val countUser = topUser.count()

    sc.stop()

    println(s"关注别人最多用户 = ${max.toMap} , 关注别人最少用户 = ${min.toMap}, 相差 = $sub" )
    println(s"被关注最多: ")
    var countIdx = 0
    for ( i  <- 0 until top10.length) {
      println ( (i+1) + " : " + top10(i) )
    }
    println(s"总用户个数是: $countUser")

  }

  def main(args: Array[String]) {
    val path = "/home/YiRan/workspace/iwantfind-tool/new2.json";
    val ret = process(path)
  }

}
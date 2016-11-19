/**
  * Created by wuyiran on 11/18/16.
  */
package com.iwantfind

import org.apache.spark.{SparkConf, SparkContext}

import org.apache.hadoop.conf._


/** Computes an approximation to pi */
object Analyser {
  // check
  def isValidate(line: String): Boolean = {
    line.split(' ').size >= 4
  }
  // return ip
  def parseIP (line :String):String = {
    val array = line.split(' ')
    if (array.isEmpty) {
      ""
    } else {
      array(0)
    }
  }

  def process (path: String) = {

    // 1. 统计目录文件大小 , 字节单位
    val hdfs = new HDFSUtils(new Configuration())
    val x = hdfs.du ( path)

    // 2. 统计总条数, 总日志行数
    val sc = new SparkContext ("local", "test", new SparkConf())
    val fileSet = sc.textFile(path)
    val lineCount = fileSet.count

    // 3. 统计有效总条数, 有效
    val vdata = fileSet.filter(isValidate(_))
    vdata.foreach(println)
    val vcount = vdata.count()

    // 4. 统计Merge后的条数

    // 5. 统计IP 个数
    val ipcountRDD = vdata.map(xb => (parseIP(xb),1)).reduceByKey(_ + _)
    val ipcount = ipcountRDD.count()

    // 7. 统计访问最多的关键词 TOP 100
    val top100 = ipcountRDD.take(100).mkString(";")

    sc.stop()
    Some(x, lineCount, vcount, ipcount, top100)
  }

  def main(args: Array[String]) {

    println("Processing " + args(0))
    val ret = process(args(0))
    ret.foreach(println)
  }

}
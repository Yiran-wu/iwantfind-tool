/**
  * Created by wuyiran on 11/18/16.
  */
package com.iwantfind

import org.apache.hadoop.conf._
import org.apache.spark.{SparkConf, SparkContext}


/** Computes an approximation to pi */
object CollectAnalyser {
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


    // 3. 有效日志行数
    val vcount = fileSet.filter(isValidate(_)).count()

    // 目录总大小, 总条数, 有效总条数
    Some(x, lineCount, vcount)
  }

  def main(args: Array[String]) {

    println("Processing " + args(0))
    val ret = process(args(0))

    ret.foreach(println)
  }

}
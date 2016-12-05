/**
  * Created by wuyiran on 11/18/16.
  */
package com.iwantfind

import org.apache.spark.{SparkConf, SparkContext}

import org.apache.hadoop.conf._


/** Computes an approximation to pi */
object AnalyserNew {
  // check
  def isValidate(line: String): Boolean = {
    //line.split(' ').size >= 4
    line.contains("jobId=")
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

  def parseJOBID (line : String) : String = {
    val array = line.split('=')
    if (array.isEmpty) {
      ""
    }else {
      array(4)
    }
  }

  def parseLineTime(line: String) : String = {
    val array = line.split(' ')
    if (array.isEmpty) {
      ""
    }else {
      array(0).trim().split('.')(0).split('T')(1)
    }

  }

  def process (path: String) = {

    val sc = new SparkContext ("local", "test", new SparkConf())
    val fileSet = sc.textFile(path)
    val lineCount = fileSet.count

    // 3. 统计有效总条数, 有效
    val vdata = fileSet.filter(isValidate(_))

    val ipcountRDD = vdata.map(xb => (parseJOBID(xb),1)).reduceByKey(_ + _).sortBy(_._2, false)

    ipcountRDD.repartition(1).saveAsTextFile("/testdata/log.out")

    //
    val icount = vdata.map(xb => (parseLineTime(xb), 1)).reduceByKey(_ + _).sortByKey()


    sc.stop()

    Some(ipcountRDD, icount)
  }

  def main(args: Array[String]) {

    val x = "[2016-12-05T10:08:28.425+08:00] [INFO] namenode.FSNamesystem.audit.logAuditEvent(FSNamesystem.java:7190) [IPC Server handler 132 on 8020] : allowed=true\tugi=dd_edw (auth:SIMPLE)    erp=yarn\tjobId=job_1479478242595_1024728\tip=/172.22.174.15\tcmd=open\tsrc=/user/dd_edw/warehouse/gdm/gdm_m04_ord_sum/dp=ACTIVE/dt=4712-12-31/000900_0.lzo\tdst=null\tperm=null"
    println(parseJOBID(x))
    println(parseLineTime(x))
    val ret = process(x)

    //

//    ret.foreach(println)
  }

}
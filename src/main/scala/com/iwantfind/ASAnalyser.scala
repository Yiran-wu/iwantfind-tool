/**
  * Created by wuyiran on 11/18/16.
  */
package com.iwantfind

import org.apache.hadoop.conf._
import org.apache.spark.{SparkConf, SparkContext}


/** Computes an approximation to pi */
object ASAnalyser {
  // check
  def isValidate(line: String): Boolean = {
    line.split('\t').size >= 3
  }
  // return ip
  def parseIP (line :String):String = {
    if (!line.contains("client_ip")) {
      ""
    } else {
      line.split("client_ip=")(1).split("&")(0)
    }
  }

  def process (path: String) = {

    // 1. 统计目录文件大小 , 字节单位
    val hdfs = new HDFSUtils(new Configuration())
    val x = hdfs.du ( path)

    // 2. 统计总条数, 总日志行数
    val sc = new SparkContext ( new SparkConf())
    val fileSet = sc.textFile(path)
    val lineCount = fileSet.count

    // 3. 统计有效总条数, 有效
    val vdata = fileSet.filter(isValidate(_))

    val vcount = vdata.count()

    // 4. 统计Merge后的条数

    // 5. 统计IP 个数
    val ipcountRDD = vdata.map(xb => (parseIP(xb),1)).reduceByKey(_ + _)
    val sortByValue = ipcountRDD.collect().sortWith(_._2 > _._2)

    val ipcount = ipcountRDD.count()

    // 7. 统计访问最多的关键词 TOP 100
    val top100 = sortByValue.take(10).mkString(";")

    sc.stop()
    Some(x, lineCount, vcount, ipcount, top100)
  }

  def main(args: Array[String]) {

//    val xx = "2016-11-14 00:00:00 - rule={allow_risk_album=0&app_platform_id=xx_xx&area=&category=1&city_info=CN_1_12_1&client_ip=10.130.92.123&contentRating=&country_code=cn&ct=album&fitAge=&from=mobile_10&isEnd=&isHomemade=&ispay=0,1&lang=zh_cn&payPlatform=&playStreams=&pn=1&ps=30&pushFlag=420003,420004&releaseYearDecade=&repo_type=0&s=mobile&src=1&stf=daycount&stt=1&style=&stype=1&subCategory=30017&uid=169068096&videoType=&vtypeFlag=0&}\tdisplay_rst={eid:6609692477932320902;experiment_id:110:1;trigger:language_boost;cache_key:1864164787730593135;is_generalized:false;is_starring_query:false;has_correct_high_search:false;has_correct_middle_search:false;has_resumed_search:true;has_rewrite_search:false;has_entity_search:false;rst_list:album_10030957(0),album_10027676(0),album_10021192(0),album_10026608(0),album_10016767(0),album_10018367(0),album_10029225(0),album_10016756(0),album_10020724(0),album_10025427(0),album_10021319(0),album_10020089(0),album_10015267(0),album_10030853(0),album_10020930(0),album_10026104(0),album_10022227(0),album_10030922(0),album_10031436(0),album_10000136(0),album_41248(0),album_10003818(0),album_10009482(0),album_10022684(0),album_10009196(0),album_10030640(0),album_75885(0),album_10024753(0),album_10031606(0),album_90649(0),;promotion_list:;group_info:(album,1,427,30,0),}\tstat={RiskControllerTime:0,qpTime:0,ipWhitelistTime:0,dispatch_0_bsTime_0:43,dispatch_0_bsTime_1:2,dispatch_0_bsTime_2:2,dispatch_0_rbsTime_0:3,dispatch_0_rbsTime_1:1,dispatch_0_rbsTime_2:1,dispatchAND:43,leafTotalTime:43,MergeResultTime:1,DoFilterTime:1,AggregatorTime:0,SeparateLiveData:0,AggregateLiveData:0,BasicSortTime:0,PreTunerTime:0,PagedResultTime:0,PromotionManagerTime:0,FindDirectItemTime:0,GetDsDataTime:15,PostTunerTime:0,ManualModifyTime:1,BuildJsonTime:11,ResponseTime:61,result:427,cost:72}"
//
//    println(parseIP(xx))
    println("Processing " + args(0))
    val ret = process(args(0))
    ret.foreach(println)
  }

}
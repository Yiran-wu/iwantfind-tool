/**
  * Created by wuyiran on 11/18/16.
  */
package com.iwantfind

import java.sql.DriverManager

import org.apache.hadoop.conf._
import org.apache.spark.{SparkConf, SparkContext}
import java.sql.Connection



/** Computes an approximation to pi */
object DBUtils {
  val url = "jdbc:mysql://localhost:3306/test"
  val username = "root"
  val password = "admin"

  classOf[com.mysql.jdbc.Driver]

  def getConnection(): Connection = {
    DriverManager.getConnection(url, username, password)
  }

  def close(conn: Connection): Unit = {
    try{
      if(!conn.isClosed() || conn != null){
        conn.close()
      }
    }
    catch {
      case ex: Exception => {
        ex.printStackTrace()
      }
    }
  }

}
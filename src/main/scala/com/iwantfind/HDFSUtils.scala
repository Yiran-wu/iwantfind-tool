package com.iwantfind

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileUtil, Path, FileSystem}



/**
  * Created by wuyiran on 11/19/16.
  */

class HDFSUtils(conf: Configuration) {
  val fileSystem = FileSystem.get(conf)

  def ls(path:String)=
  {
    println("list path:"+path)
    val fs = fileSystem.listStatus(new Path(path))
    val listPath = FileUtil.stat2Paths(fs)
    for( p <- listPath)
    {
      println(p)
    }
    println("----------------------------------------")
  }

  def du(xpath: String) : Long = {
    var size : Long = 0
    val path = new Path(xpath)
    if (fileSystem.exists(path)) {
      var dir = fileSystem.getContentSummary(path)
      dir.getLength
    }

    size
  }
}

package com.test.utils

import java.io.File
import java.nio.file.Files

object LocalFileUtils {
  //  做本地调试使用，量产环境使用的是hdfs
  def deleteDir(dir: File): Unit = {
    if (!dir.exists()) {
      return
    }
    val files = dir.listFiles()

    files.foreach(f => {
      if (f.isDirectory) {
        deleteDir(f)
      } else {
        f.delete()
        //        println("delete file " + f.getAbsolutePath)
      }
    })
    dir.delete()
    println("delete dir " + dir.getAbsolutePath)
  }

}

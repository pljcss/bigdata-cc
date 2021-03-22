package com.core.scala

import java.io.File

/**
 *
 * @author cs
 * @date 2020/12/12 5:19 下午
 */
object Test01 {
  def main(args: Array[String]): Unit = {


    val file = new File("output_job")
    println(file.exists())
    if (file.exists()) {
      print("----删除文件------")
      val files = file.listFiles()
      files.foreach(_.delete())
    }

    def deleteDir(filePath: String): Unit = {

    }


  }

}

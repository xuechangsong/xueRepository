package com.atbeiming.wordcount


import org.apache.spark.{SparkConf, SparkContext}

object test {
    def main(args: Array[String]): Unit = {

//      1.根据身份证号判断并识别出用户的年龄段（如60后70后，80后......）
//      2.将判断的结果追加到每位用户信息的后面，实现[编号，身份证号，用户姓名，年龄段]
//      3.将最后的结果保存到新的文件d://userinfo.txt

      val sparkConf = new SparkConf().setAppName("mytest").setMaster("local[*]")
      val sc = new SparkContext(sparkConf)
      sc.setLogLevel("ERROR")
      val file = sc.textFile("E:\\idea\\spark\\data\\mytest.txt")
      val line = file.flatMap(_.split(" "))
      //val idrdd = words.toString().map(x => (x._1,x._2))

      val disrdd = line.filter(_.length > 3 )
      line.foreach(println(_))

      println(line.getNumPartitions)
      sc.stop()

    }
}

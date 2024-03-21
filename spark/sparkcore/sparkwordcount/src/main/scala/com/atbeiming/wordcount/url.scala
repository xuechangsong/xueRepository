package com.atbeiming.wordcount

import org.apache.spark.{SparkConf, SparkContext}

object url {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("url").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    val file = sc.textFile("E:\\idea\\spark\\data\\mytest.txt")

    //过滤长度为0，不包含GET,POST的url
    //val filterrdd = file.filter(_.length() > 0).filter(line => (line.indexOf("GET") > 0 || line.indexOf("POST") > 0 ))
    val filterrdd = file.filter(line => (line.contains("GET") || line.contains("POST")))
    val result = filterrdd.map(line => {
      if(line.contains("GET")){
      //if(line.indexOf("GET") > 0){
        (line.substring(line.indexOf("GET"),line.indexOf("HTTP/1.0")).trim,1)
      }
      else {
        (line.substring(line.indexOf("POST"),line.indexOf("HTTP/1.0")).trim,1)
      }
    }).reduceByKey((x,y) => (x+y))

    result.foreach(println)
  }

}

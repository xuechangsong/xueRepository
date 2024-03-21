package com.atbeiming.wordcount

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}


object wordcount {
    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setAppName("wordcount").setMaster("local[*]")
        val sc = new SparkContext(sparkConf)
        //val path = sc.setCheckpointDir()
        val sparkSession = SparkSession.builder().master("local").appName("wordcount")
        val file = "E:\\idea\\spark\\data\\mytest.txt"
        //val result = sc.textFile(file).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).sortBy(_._2,false)
        val filerdd = sc.textFile(file, 5)
        val words = filerdd.flatMap(_.split(" "))
        val disword = words.distinct()
        words.cache() //words.persist()    //words.checkpoint()
        val word2count = words.map((_, 1))
        val result = word2count.reduceByKey((x, y) => (x + y))
        //num,num2获取rdd分区数量
        val num = filerdd.getNumPartitions
        val num2 = result.partitions.size
        val num3 = result.partitions.length
        println(num)
        println(num2)
        println(num3)
        //result.foreach(println(_))
        //result.saveAsTextFile("D:/ideal/spark/data/count2.txt")
        sc.stop()

    }

}

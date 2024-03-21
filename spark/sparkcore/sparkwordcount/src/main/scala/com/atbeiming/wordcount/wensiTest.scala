package com.atbeiming.wordcount

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object wensiTest {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("mytest").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.textFile("E:\\idea\\spark\\data\\wensidata.txt")
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    import spark.implicits._
    val df = rdd.map(_.split(",")).map(paras => (paras(0),paras(1),paras(2),paras(3))).toDF("peer_id","id_1","id_2","year")
    df.createOrReplaceTempView("data")

    //1.计算peer_id包含id_2的年份
    val filterDf = spark.sql("select peer_id,year from data where peer_id like concat('%',id_2,'%')")
    filterDf.show()
    //2.对于每个pee_id,计算小于等于步骤一年份的数量
    val countDF = spark.sql("select a.peer_id,a.year,count(1) from data a join (select peer_id,year from data where " +
      "peer_id like concat('%',id_2,'%')) b on a.peer_id = b.peer_id where a.year <= b.year group by a.peer_id,a.year")
    countDF.show()
    //3.按照年份排序
    val countYearDF = spark.sql("select a.peer_id,a.year,count(1) cnt from data a join (select peer_id,year from data where" +
      " peer_id like concat('%',id_2,'%')) b on a.peer_id = b.peer_id where a.year <= b.year group by a.peer_id,a.year order by a.year")
    countYearDF.show()
    countYearDF.createOrReplaceTempView("count")

    val count2021DF = spark.sql("select year,sum(cnt) from count where year = '2021' group by year")
    count2021DF.show()
    val firstRow = count2021DF.first()
    val firstRDD = count2021DF.rdd
    val year = firstRDD.collect()(0)
    val cnt = firstRDD.collect()(1)

    }










  }


}

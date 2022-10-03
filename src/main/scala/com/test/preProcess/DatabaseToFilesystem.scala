package com.test.preProcess

import com.test.utils.{Constants, LocalFileUtils, PropertyUtil, StringUtil}
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.io.File
import java.util.Properties
import scala.collection.mutable
import scala.collection.mutable.Set

/**
 * 读取数据库的数据，把结果放到hdfs（如果使用本地测试则放置在项目目录下）的目录中
 * 生成forMerge目录
 * id：号码1 证件2 号码3 ...
 * idea本地调试参数：conf/test.properties tmp_data/preProcess
 */
object DatabaseToFilesystem {
  def main(args: Array[String]): Unit = {
    val textFilePath = args(0)
    val hdfsPath = args(1)

    val properties = PropertyUtil.getProperties(textFilePath)
    val jdbcProps = new Properties()
    jdbcProps.put("url", properties.getProperty("url"))
    jdbcProps.put("driver", properties.getProperty("driver"))
    jdbcProps.put("user", properties.getProperty("user"))
    jdbcProps.put("password", properties.getProperty("password"))

    val sparkConf = new SparkConf()
    /**
     * 生产环境使用
     */
    //    val spark = SparkSession.builder().appName("Preprocess").enableHiveSupport().config(sparkConf).getOrCreate()
    //    测试需要在run configuration 添加参数：conf/test.properties tmp_data/preprocess

    //    分区表使用，用于加快数据读取
    val predicates =
      Array(
        "19900101" -> "20050101",
        "20050101" -> "20060101",
        "20060101" -> "20070101",
        "20070101" -> "20080101",
        "20080101" -> "20090101",
        "20090101" -> "20100101",
        "20100101" -> "20110101",
        "20110101" -> "20120101",
        "20120101" -> "20130101",
        "20130101" -> "20140101",
        "20140101" -> "20150101",
        "20150101" -> "20160101",
        "20160101" -> "20170101",
        "20170101" -> "20180101",
        "20180101" -> "20190101",
        "20190101" -> "20200101",
        "20200101" -> "20210101",
        "20210101" -> "20220101",
        "20220101" -> "20230101",
        "20230101" -> "20240101",
        "20240101" -> "20250101",
        "20250101" -> "20260101",
        "20260101" -> "20270101",
        "20270101" -> "20280101",
        "20280101" -> "20290101",
        "20290101" -> "20300101",
        "20300101" -> "20310101",
        "20310101" -> "20320101",
        "20320101" -> "20330101",
        "20330101" -> "20340101",
        "20340101" -> "20350101",
        "20350101" -> "20360101",
        "20360101" -> "20370101",
        "20370101" -> "20380101",
        "20380101" -> "20390101",
        "20390101" -> "20400101",
        "20400101" -> "20410101",
        "20410101" -> "20420101",
        "20420101" -> "20430101",
        "20430101" -> "20440101",
        "20440101" -> "20450101",
        "20450101" -> "20460101",
        "20460101" -> "20470101",
        "20470101" -> "20480101",
        "20480101" -> "20490101",
        "20490101" -> "20500101"
      ).map {
        case (start, end) =>
          s"slrq >= '$start'::date AND slrq < '$end'::date"
      }

    /**
     * 生产环境读取数据库
     *
     */
/*
    val increaseRdd: RDD[(String, mutable.Set[String])]
    = spark.read.jdbc(jdbcProps.getProperty("url"), tableName, predicates, jdbcProps).rdd.map(r => {
      val recordId = getStrDefault(r.getAs[String]("mr_id_sq"))
      val sfzh = getStrDefault(r.getAs[String]("sfzjxx"))
      val gazj = getStrDefault(r.getAs[String]("gazjxx"))
      val xczj = getStrDefault(r.getAs[String]("xczjxx"))
      var sbzj = getStrDefault(r.getAs[String]("sbzjxx"))
      val set = Set(sfzh, gazj, xczj, sbzj).filter(StringUtil.isNotNull(_))
      (recordId, set)
    }).filter(x => x._2 != null && !x._2.isEmpty).repartition(10).cache()
*/

    /**
     * 测试环境使用
     */
    val spark = SparkSession.builder().appName("Preprocess").master("local[*]").config(sparkConf).getOrCreate()
    val tableName = properties.getProperty("tableName")

    /**
     * 以下是测试代码
     * 手动生成数据
     */
    val sc = spark.sparkContext
    val resultPath = "tmp_data/result/history"
    LocalFileUtils.deleteDir(new File(resultPath))
    //  测试代码:初始化历史的内容-begin
    val histroyList = List(
      ("10001", Set("sfzh10001", "gazj10001", "xczj10001", "sbzj10001"))
    )
    val historyRdd = sc.makeRDD(histroyList)
    println("historyRdd: ")
    historyRdd.foreach(println)
    historyRdd.map(ele => {
      val id = ele._1
      val hmString = ele._2.toList.sorted.mkString(" ")
      id + ":" + hmString
    })
      .saveAsTextFile(resultPath, classOf[GzipCodec])
    //  测试代码:初始化历史的内容-end

    LocalFileUtils.deleteDir(new File(hdfsPath))
    val increaseList = List(
      ("10002", Set("sfzh10002", "gazj10002", "xczj10002", "sbzj10002"))
      , ("10003", Set("sfzh10001", "gazj10003", "xczj10003", "sbzj10003"))
      , ("10004", Set("sfzh10002", "gazj10004", "xczj10004", "sbzj10004"))
    )
    val increaseRdd = sc.makeRDD(increaseList) // 测试代码
    println("increaseRdd: ")
    increaseRdd.foreach(println)
    increaseRdd.map(ele => {
      val id = ele._1
      val hmSet = ele._2
      //  替换掉字符串中的空格
      val regStr = "\\s"
      val hmString = hmSet.map(hm => hm.replaceAll(regStr, "")).toList.sorted.mkString(" ")
      id + ":" + hmString
    })
      //      .saveAsTextFile(hdfsPath + "/merge", classOf[GzipCodec]);
      .saveAsTextFile(hdfsPath + "/forMerge", classOf[GzipCodec]); // 测试代码
  }

  def getStrDefault(str: String): String = {
    if (str == null) null else str
  }
}

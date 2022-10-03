package com.test.IdMergeInOne

import com.test.utils.{Constants, LocalFileUtils}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import com.test.IdMergeInOne.IDsMergeFunc

import java.io.File
import java.util.StringTokenizer
import scala.collection.mutable

/**
 * 把有相同证件id的多条数据融合为1条数据，最终生成的数据在hdfs的{idmergeOutput}/result目录下
 * 生成数据格式 证件id1+空格+证件id2+....
 * idea本地调试参数：tmp_data/preProcess/ tmp_data/merge/ tmp_data/result/ 10
 */
object IDsMergeEntry {
  def main(args: Array[String]): Unit = {
    val incrPath = args(0)
    val idsMergeOutputParent = args(1)
    val resultPath = args(2)
    val threshold = args(3).toInt
    val incrFiles = incrPath + "forMerge/*.gz"
    val histFiles = resultPath + "history/*.gz"

    //    val sc = new SparkContext( new SparkConf().setAppName("IDsMerge"));
    //    测试需要在run configuration
    //    添加参数：tmp_data/preprocess/forMerge/*.gz tmp_data/result/history/*.gz tmp_data/merge/ 10
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("IDsMerge")); // 测试代码
    sc.setLogLevel("WARN")
    LocalFileUtils.deleteDir(new File(idsMergeOutputParent))
    val defaultParallelism = sc.defaultParallelism

    val worker = new IDsMergeFunc();

    val incrSrcRdd: RDD[String] = sc.textFile(incrFiles)
    val incrPreRdd: RDD[(String, mutable.Set[String])] = worker.parseTextLine(incrSrcRdd)
    //    println("incrPreRdd: ")
    //    incrPreRdd.foreach(println)
    //    (10003,Set(sbzj10003, gazj10003, sfzh10001, xczj10003))
    //    (10004,Set(xczj10004, gazj10004, sbzj10004, sfzh10002))
    //    (10002,Set(sfzh10002, xczj10002, sbzj10002, gazj10002))

    val histSrcRdd: RDD[String] = sc.textFile(histFiles)
    val histPreRdd: RDD[(String, mutable.Set[String])] = worker.parseTextLine(histSrcRdd)
    //    println("histPreRdd: ")
    //    histPreRdd.foreach(println)
    //    (10001,Set(gazj10001, xczj10001, sbzj10001, sfzh10001))


    val mixRdd: RDD[(String, (mutable.Set[String], String))] =
      worker.idsMix(histPreRdd, incrPreRdd, defaultParallelism)


    worker.encodeIds(mixRdd, sc.defaultParallelism, idsMergeOutputParent, threshold);

    val lineEncodedRawRdd: RDD[String] = sc.textFile(idsMergeOutputParent + "lineEncoded/*.gz")
    //    println("lineEncodedRawRdd: ")
    //    lineEncodedRawRdd.foreach(println)
    //    10002|i:1 4 6 10
    //    10004|i:6 7 9 12
    //    10003|i:2 3 5 11
    //    10001|h:0 2 8 13

    val lineEncodedPreRdd = worker.parseTextLine(lineEncodedRawRdd).map(line => {
      val idbz = line._1
      val idbzArr = idbz.split("\\|")
      val id = idbzArr(0)
      val bz = idbzArr(1)
      val hmEncodedSet = line._2
      (id, (hmEncodedSet, bz))
    })
    //    println("lineEncodedPreRdd: ")
    //    lineEncodedPreRdd.sortBy(ele => ele._1, ascending = true, numPartitions = 1).foreach(println)
    //    (10001,(Set(0, 8, 2, 13),h))
    //    (10002,(Set(4, 1, 10, 6),i))
    //    (10003,(Set(3, 5, 2, 11),i))
    //    (10004,(Set(9, 6, 7, 12),i))

    val hmEncodingRawRdd = sc.textFile(idsMergeOutputParent + "hmEncoded/*.gz");
    val hmEncodingPreRdd = hmEncodingRawRdd.map(line => {
      val lineArr = line.split(" ")
      val hm = lineArr(0)
      val hmEncoded = lineArr(1)
      (hm, hmEncoded)
    })
    //    println("hmEncodingPreRdd: ")
    //    hmEncodingPreRdd.sortBy(ele => ele._2.toInt, ascending = true, numPartitions = 1).foreach(println)
    //    (gazj10001,0)
    //    (xczj10002,1)
    //    (sfzh10001,2)
    //    (xczj10003,3)
    //    (gazj10002,4)
    //    (gazj10003,5)
    //    (sfzh10002,6)
    //    (xczj10004,7)
    //    (sbzj10001,8)
    //    (gazj10004,9)
    //    (sbzj10002,10)
    //    (sbzj10003,11)
    //    (sbzj10004,12)
    //    (xczj10001,13)

    worker.idsMerge(lineEncodedPreRdd, idsMergeOutputParent + "resultEncoded/", defaultParallelism, threshold);
    val resultEncodedRawRdd = sc.textFile(idsMergeOutputParent + "resultEncoded/*/*.gz");
    val resultEncodedPreRdd = worker.parseTextLine(resultEncodedRawRdd)
    //    println("resultEncodedPreRdd: ")
    //    resultEncodedPreRdd.sortBy(line=>line._1).foreach(println)
    //    (10001,Set(3, 0, 8, 5, 2, 13, 11))
    //    (10002,Set(4, 1, 10, 9, 6, 7, 12))

    worker.decodeIds(resultEncodedPreRdd, hmEncodingPreRdd, defaultParallelism, resultPath, threshold)
    //    val resultDecodingRawRdd = sc.textFile(idsMergeOutputParent + "resultDecoded/*.gz");
    //    println("resultDecodedRawRdd: ")
    //    resultDecodingRawRdd.sortBy(line=>line.split(":")(0),ascending = true,numPartitions = 1).foreach(println)
    //    10001:gazj10001 gazj10003 sbzj10001 sbzj10003 sfzh10001 xczj10001 xczj10003
    //    10002:gazj10002 gazj10004 sbzj10002 sbzj10004 sfzh10002 xczj10002 xczj10004

    sc.stop
  }

}
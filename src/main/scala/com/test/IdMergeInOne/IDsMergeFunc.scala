package com.test.IdMergeInOne

import com.test.utils.{Constants, LocalFileUtils, MD5Util, StringsRandom}
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import java.util
import java.util.StringTokenizer
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer, Set}

class IDsMergeFunc {
  /*  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("IDsMergeTesting")); // 测试代码
    sc.setLogLevel("WARN")
  }*/

  /**
   * 将 ID：号码1 号码2 号码3 ... 格式的数据解析成为RDD[String]): RDD[(String, mutable.Set[String])]的格式
   *
   * @param lineRdd
   * @return
   */
  def parseTextLine(lineRdd: RDD[String]): RDD[(String, mutable.Set[String])] = {
    lineRdd.map(line => {
      val lineArr = line.split(":")
      val id = lineArr(0)
      val hmsStr = lineArr(1)
      val hmsSet = new mutable.HashSet[String]()
      val st = new StringTokenizer(hmsStr)
      while (st.hasMoreTokens) {
        hmsSet += st.nextToken()
      }
      (id, hmsSet)
    })
  }

  /**
   * 将 号码 信息编码成为数字信息，见减小数据计算时候的内存占用量
   *
   * @param lineRdd            需要编码的rdd
   * @param defaultParallelism 并行度
   * @param idmergeOutput      数据输出根目录
   * @param threshould         阈值，用于过滤掉太长（异常的）的数据
   */
  def encodeIds(lineRdd: RDD[(String, (mutable.Set[String], String))], defaultParallelism: Int, idmergeOutput: String, threshould: Int): Unit = {
    val lineEncodingPath = idmergeOutput + "lineEncoded"
    val hmEncodingPath = idmergeOutput + "hmEncoded"
    //    lineRdd.foreach(println)

    val hmRdd: RDD[String] =
      lineRdd.map(ele => {
        val hmList = ele._2._1.toList
        hmList
      }).filter(hmList => hmList.length < threshould /*&& arr.length >1*/).flatMap(x => x)
        .repartition(defaultParallelism)
        .persist(StorageLevel.MEMORY_AND_DISK_SER);
    //    println("data-flatmap: ")
    //    data.flatMap(ele=>ele).zipWithIndex().sortBy(x=>x._2,ascending = true,numPartitions = 1).foreach(println)

    val hnEncodingRdd = hmRdd.distinct()
      .zipWithIndex()
      .repartition(defaultParallelism)
      .persist(StorageLevel.MEMORY_AND_DISK_SER);

    hnEncodingRdd.map {
      case (hm, code) =>
        s"$hm ${code.toInt}";
    }.saveAsTextFile(hmEncodingPath, classOf[GzipCodec]);

    val hmIdPair: RDD[(String, (String, String))] =
      lineRdd.flatMap(ele => {
        val id = ele._1
        val hmSet = ele._2._1
        val bz = ele._2._2
        val res = new mutable.ListBuffer[(String, (String, String))]
        for (hm <- hmSet) {
          res.append((hm, (id, bz)))
        }
        res
      }).partitionBy(new HashPartitioner(defaultParallelism))

    hmIdPair.join(hnEncodingRdd).map(e => {
      val hm = e._1
      val id = e._2._1._1
      val bz = e._2._1._2
      val idbz = id + "|" + bz
      val hmEncoding = e._2._2
      (idbz, hmEncoding)
    }).groupByKey(defaultParallelism)
      .map { case (idbz, hmArr) =>
        idbz ++ ":" ++ hmArr.toList.sorted.mkString(" ")
      }
      .saveAsTextFile(lineEncodingPath, classOf[GzipCodec]);
  }

  /**
   * 用于将 ID:编码号码1 编码号码2 编码号码3 ... 转化为 ID:号码1 号码2 号码3 ...
   *
   * @param idHmEncodedRawRdd   id和编码号码的的rdd
   * @param hmEncodedRawRdd     号码和编码号码的rdd
   * @param defaultParallelism  并行度
   * @param idmergeResultOutput 数据根目录
   * @param threshould          阈值
   */
  def decodeIds(idHmEncodedRawRdd: RDD[(String, mutable.Set[String])]
                , hmEncodedRawRdd: RDD[(String, String)]
                , defaultParallelism: Int, idmergeResultOutput: String, threshould: Int): Unit = {
    val idHmEncodedFlatRdd: RDD[(String, String)] =
      idHmEncodedRawRdd
        .flatMap(ele => {
          val id = ele._1
          val hmEncodedSet = ele._2
          val tmpArr = new ArrayBuffer[(String, String)]()
          for (hmEncoded <- hmEncodedSet) {
            tmpArr.append((hmEncoded, id))
          }
          tmpArr
        })
        .partitionBy(new HashPartitioner(defaultParallelism))
        .persist(StorageLevel.MEMORY_AND_DISK_SER);
    //    println("idHmEncodedFlatRdd: ")
    //    idHmEncodedFlatRdd.sortBy(x => (x._2, x._1), ascending = true, numPartitions = 1).foreach(println)

    val hmEncoderPreRdd: RDD[(String, String)] =
      hmEncodedRawRdd
        .map(ele => {
          val hm = ele._1
          val hmEncoded = ele._2
          (hmEncoded, hm)
        })
        .partitionBy(new HashPartitioner(defaultParallelism))
        .persist(StorageLevel.MEMORY_AND_DISK_SER);
    //    println("hmEncoderPreRdd: ")
    //    hmEncoderPreRdd.foreach(println)


    val rdd = idHmEncodedFlatRdd.join(hmEncoderPreRdd)
      .map(ele => {
        val id = ele._2._1
        val hm = ele._2._2
        //  此处转为含有单个号码的Set方便下一步进行聚合的，取集合的合集
        (id, Set(hm))
      })
    rdd.reduceByKey(_ ++ _)
      .filter(ele => {
        val hmSet = ele._2
        hmSet.size < threshould
      })
      .map(ele => {
        val id = ele._1
        val hmSet = ele._2.toList.sorted
        val idsSB = new StringBuilder();
        val hmString = hmSet.mkString(" ")
        val line = id + ":" + hmString
        line
      })
      //      .foreach(println)
      .saveAsTextFile(idmergeResultOutput + "new/", classOf[GzipCodec]);
  }

  /**
   * 历史和新增合并后并添加标志 "i"-新增的  或者  "h"-历史的
   *
   * @param historyLine        历史的已生成的id和号码的数据
   * @param increaseLine       需要新增计算的id和号码的数据
   * @param defaultParallelism 并行度
   * @return 历史和新增合并后的rdd(注意此处并不祛重，不涉及shuffer)
   */
  def idsMix(historyLine: RDD[(String, mutable.Set[String])], increaseLine: RDD[(String, mutable.Set[String])]
             , defaultParallelism: Int): RDD[(String, (mutable.Set[String], String))] = {
    var hisLine: RDD[(String, (mutable.Set[String], String))] = null
    var incLine: RDD[(String, (mutable.Set[String], String))] = null
    if (historyLine != null && !historyLine.isEmpty()) {
      hisLine = historyLine.map(ele => {
        val id = ele._1
        val hmSet = ele._2
        val bz = "h"
        (id, (hmSet, bz))
      })
    }
    if (increaseLine != null || !increaseLine.isEmpty()) {
      incLine = increaseLine.map(ele => {
        val id = ele._1
        val hmSet = ele._2
        val bz = "i"
        (id, (hmSet, bz))
      })
    }
    //    ready to encode hm information
    val preRdd: RDD[(String, (mutable.Set[String], String))] =
      hisLine.union(incLine).repartition(defaultParallelism)
    preRdd
  }

  /**
   * 主计算过程
   *
   * @param mixRdd             混合的数据（含历史的和新增的）
   * @param IDsMergeOutput     结果输出根路径
   * @param defaultParallelism 并行度
   * @param threshold          阈值
   */
  def idsMerge(mixRdd: RDD[(String, (mutable.Set[String], String))]
               , IDsMergeOutput: String, defaultParallelism: Int, threshold: Int) {
    val IDsMergeOutputPath = IDsMergeOutput + "loop"
    val idFlatRddInit: RDD[(String, (mutable.Set[String], String, Int, String))] =
      mixRdd.flatMap(ele => {
        val id = ele._1
        val bz = ele._2._2
        val hmSet = ele._2._1
        val tmpList = new ListBuffer[(String, (mutable.Set[String], String, Int, String))]
        for (hm <- hmSet) {
          tmpList.append((hm, (hmSet, id, 0, bz)))
        }
        tmpList
      }).repartition(defaultParallelism)

    var idFlatRdd = idFlatRddInit;
    var loopCount = 0;
    while (true) {
      //   pre_rdd:RDD[(String, (mutable.Set[String], Int, String))]
      //   聚合计算，并生成聚合次数。
      idFlatRdd = idFlatRdd
        .reduceByKey((x, y) => {
          //  两个set 相互聚合
          val xyset = x._1.union(y._1);
          val xySize = xyset.size;
          val xSize = x._1.size;
          val ySize = y._1.size;
          var id = ""
          var bz = ""
          if ("i".equals(x._4) && "i".equals(y._4)) {
            //  前后两条都代表（不一定是）增量数据，历史数据的id取较小者
            id = if (x._2 > y._2) y._2 else x._2
            bz = "i"
          } else if ("i".equals(x._4) && "h".equals(y._4)) {
            //  x代表增量数据，y是历史数据。有历史数据的id 优先使用历史数据id
            id = y._2
            bz = "h"
          } else if ("h".equals(x._4) && "i".equals(y._4)) {
            //  x代表历史数据，y是增量数据。有历史数据的id 优先使用历史数据id
            id = x._2
            bz = "h"
          } else if ("h".equals(x._4) && "h".equals(y._4)) {
            //  前后两条都代表（不一定是）历史数据，历史数据的id取较小者(遗留：这里会造成已经生成的Id消失的问题！！！)
            id = if (x._2 > y._2) y._2 else x._2
            bz = "h"
          }
          if (xySize.equals(xSize) || xySize.equals(ySize)) {
            (xyset, id, Math.max(x._3, y._3), bz)
          } else {
            (xyset, id, Math.min(x._3, y._3) + 1, bz)
          }
        })
        .cache();
      //   idsFlat.sortBy(x=>x._1,ascending = true,numPartitions = 1).foreach(println)

      //  对于循环次数超过聚合次数的情况，说明数据已经聚合完毕，需要落到磁盘。
      idFlatRdd
        //   .sortBy(x=>x._1,ascending = true,numPartitions = 1)
        .filter(f => {
          val mergeTimes = f._2._3
          (mergeTimes + 1).equals(loopCount) && f._2._1.size <= threshold;
        })
        .map(xo => {
          val id = xo._2._2
          val idList = xo._2._1.toList.sorted.mkString(" ")
          id + ":" + idList
        }).distinct()
        //   .sortBy(x=>x._1,ascending = true,numPartitions = 1).foreach(x=>println(loopCount+" -> "+x))
        .saveAsTextFile(IDsMergeOutput + f"loop-$loopCount%3d".replaceAll(" ", "0"), classOf[GzipCodec]);

      //   对于还需要继续进行聚合的数据需要进行裂变。
      idFlatRdd = idFlatRdd
        .filter(ele => {
          val mergeTimes = ele._2._3
          (mergeTimes + 1 >= loopCount) && ele._2._1.size < threshold
        }).flatMap(ele => {
        val arr = new ArrayBuffer[(String, (mutable.Set[String], String, Int, String))]()
        val hmSet = ele._2._1
        val id = ele._2._2
        val mergeTimes = ele._2._3
        val bz = ele._2._4
        hmSet.foreach(hm => {
          arr.append((hm, (hmSet, id, mergeTimes, bz)))
        })
        arr
      })
      //      println("final idsFlat.length: "+idsFlat.collect().length)
      //      idsFlat.sortBy(x=>(x._2._1.toString(),x._1),ascending = true,numPartitions = 1).foreach(x=>println(loopCount+" -> "+x))
      loopCount = loopCount + 1;
      if (idFlatRdd.isEmpty()) {
        //   退出条件
        return
      }
    }
  }

}
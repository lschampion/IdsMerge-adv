package com.test.postProcess

import com.test.utils.{Constants, JDBCUtil, PropertyUtil, StringUtil}
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * 把IDsMergeEntry生成的数据写入数据库表中
 * idea本地调试参数：tmp_data/result/ conf/test.properties
 */
object LocalToDatabase {
  def main(args: Array[String]): Unit = {
    println("args====" + args.mkString(","))

    val resultPath = args(0)
    val resultFile=resultPath+"new/*.gz"
    val properties = PropertyUtil.getProperties(args(1))
    println("properties====" + properties)

    //    val spark = SparkSession.builder().config(new SparkConf().setAppName("HdfsToGauss")).enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder().master("local[*]").config(new SparkConf().setAppName("HdfsToDatabase")).getOrCreate() // 测试
    val sc = spark.sparkContext

    import spark.implicits._
    val resultDF: DataFrame = sc.textFile(resultFile)
      .flatMap(ele => {
        val arr = ele.split(":")
        val id = arr(0)
        val hmListStr = arr(1)
        hmListStr.split(" ").map(hm => {
          (id, hm.trim)
        })
      }).toDF("id", "hm")

//    resultDF.show()
//      +-----+---------+
//      |   id|       hm|
//      +-----+---------+
//      |10001|gazj10001|
//      |10001|gazj10003|
//      |10001|sbzj10001|
//      |10001|sbzj10003|
//      |10001|sfzh10001|
//      |10001|xczj10001|
//      |10001|xczj10003|
//      |10002|gazj10002|
//      |10002|gazj10004|
//      |10002|sbzj10002|
//      |10002|sbzj10004|
//      |10002|sfzh10002|
//      |10002|xczj10002|
//      |10002|xczj10004|
//      +-----+---------+

//    写入数据库
/*        resultDF.write.format("jdbc").mode(SaveMode.Overwrite)
          .option("driver",properties.getProperty("driver"))
          .option("url", properties.getProperty("url"))
          .option("dbtable","newimportdata.ryxx_mx")
          .option("user", properties.getProperty("user"))
          .option("password", properties.getProperty("password"))
          .option("batchsize",4000)
          .save()*/

    sc.stop
  }
}

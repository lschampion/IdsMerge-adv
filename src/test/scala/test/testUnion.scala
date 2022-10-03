package test

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object testUnion {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext( new SparkConf().setMaster("local[*]").setAppName("IDsMerge"));  // 测试代码
    sc.setLogLevel("WARN")
    val data1_rdd = sc.parallelize(List(1,2,3))
    val data2_rdd = sc.parallelize(List(3,4,5))
    val data3_rdd = sc.parallelize(List(("a",List(1,2)),("b",List(3,4)),("c",List(5,6))))
//    data1_rdd.union(data2_rdd).sortBy(x=>x,ascending = true,numPartitions = 1).foreach(println)
//    data1_rdd.zipWithIndex().foreach(println)


//    val data3_res=data3_rdd.flatMap(ele=>{
//      val id= ele._1
//      val ls=ele._2
//      val tmpList=new ListBuffer[(String,Int)]
//      for(e<-ls){
//        tmpList.append((id,e))
//      }
//      tmpList
//    })
//    data3_res.foreach(println)

//    val ls1=new ListBuffer[Int]
//    ls1.append(1,2,3,4)
//    val ls2=ls1:+5
//    print(ls1)
//    print(ls2)


    val num=1

    print(f"$num%3d".replaceAll(" ","0"))

  }

}

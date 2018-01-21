package demo

import java.util.ArrayList
import java.util.concurrent.Future

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object Spark4SingleThread {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Spark4SingleThread").master("local[1]").getOrCreate()
    val sc = SparkContext.getOrCreate()
    val list=new ArrayList[Future[String]]()
    //并行任务读取的path
    val task_paths=new ArrayList[String]()
    task_paths.add("/Users/tuotuo/data")
    task_paths.add("/Users/tuotuo/repo/xmlenc/xmlenc/0.52")
    task_paths.add("/Users/tuotuo/Movies")

    val startTime = System.currentTimeMillis() /1000
    for (i<-0 until task_paths.size()){
      val count = sc.textFile(task_paths.get(i)).count()
      val msg = task_paths.get(i) + " 的文件数量：" + count
      Thread.sleep(10000)
      println(msg)
    }

    val endTime = System.currentTimeMillis() / 1000
    println("spend time ----------------- :"+(endTime - startTime))

    sc.stop()
    spark.stop()

  }
}

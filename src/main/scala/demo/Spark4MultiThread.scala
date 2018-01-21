package demo

import java.util.ArrayList
import java.util.concurrent.{Callable, Executors, Future}

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object Spark4MultiThread {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Spark4MultiThread").master("local[1]").getOrCreate()
    val sc = SparkContext.getOrCreate()
    val list=new ArrayList[Future[String]]()
    //并行任务读取的path
    val task_paths=new ArrayList[String]()
    task_paths.add("/Users/tuotuo/data")
    task_paths.add("/Users/tuotuo/repo/xmlenc/xmlenc/0.52")
    task_paths.add("/Users/tuotuo/Movies")
    val startTime = System.currentTimeMillis() /1000
    //线程数等于path的数量
    val nums_threads=task_paths.size()
    //构建线程池
    val executors = Executors.newFixedThreadPool(nums_threads)
    for (i<-0 until nums_threads){
      val task = executors.submit(new Callable[String] {
        override def call() :String ={
          try {
            val count = sc.textFile(task_paths.get(i)).count()
            val msg = task_paths.get(i) + " 的文件数量("+i+")：" + count
            Thread.sleep(10000)
            return msg
          } catch {
            case ex: Exception => {
              ex.printStackTrace()
              return ex.getMessage
            }
          }
        }
      })
      list.add(task)
    }
    val endTime = System.currentTimeMillis() / 1000
    println("spend time ----------------- :"+(endTime - startTime))

    for (i <- 0 to list.size())
      try {
        println(list.get(i).get())
      } catch {
        case ex: Exception => {
          println(ex.getMessage)
        }
      }

    executors.shutdownNow()
    sc.stop()
    spark.stop()
  }
}

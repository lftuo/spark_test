package demo

import org.apache.spark.sql.SparkSession

object Spark4MySQL {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkSQLDemo").master("local[*]").getOrCreate()

    val jdbcDF = spark.read.format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", "jdbc:mysql://localhost:3306")
      .option("dbtable", "spider_lianjia.spider_lianjia_new_house")
      .option("user", "root")
      .option("password", "123456")
      .load()

    jdbcDF.show()
  }
}

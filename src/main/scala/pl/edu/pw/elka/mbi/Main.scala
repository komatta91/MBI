package pl.edu.pw.elka.mbi

import org.apache.spark.sql.SparkSession


object Main {

  // https://github.com/samtools/htsjdk/blob/master/src/test/java/htsjdk/tribble/index/tabix/TabixIndexTest.java
  // testQueryProvidedItemsAmount
  def main(args: Array[String]): Unit = {
    val iter = MultiFileIterator.openMultipleFiles(args.toList)
    val columns = iter.sortedColumnSet

    val spark = SparkSession.builder().appName("MBI").master("local[4]").getOrCreate()

    val startMillis = System.currentTimeMillis
    val data = VcfSparkAdapter.createDataFrame(spark, columns, iter, 100)
    println((System.currentTimeMillis - startMillis)+"ms")
    println(data.count())
  }
}
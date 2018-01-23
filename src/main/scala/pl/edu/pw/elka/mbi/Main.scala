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

    //RHM
    val iterations = 10
    val distances = 0.until(iterations).map(i => new RHMIteration(i, data).distances)
    val averageDistances = distances
      .reduce((acc, m) => acc ++ m.map { case (k,v) => k -> (v + acc.getOrElse(k,0.0)) })
    val averageDistance = averageDistances.values.sum / averageDistances.size

    val median = 1.4286 * averageDistances.toStream.sortWith((a, b) => a._2.compareTo(b._2) < 0).drop(averageDistances.size / 2).head._2
    val outliers = averageDistances.filter((entry: (String, Double)) => entry._2 > median).keySet

    println(outliers)
    println(outliers.size)
    println(averageDistances.size)

  }
}
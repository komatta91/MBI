package pl.edu.pw.elka.mbi

import org.apache.spark.sql.SparkSession
import pl.edu.pw.elka.mbi.rhm.RHMClassifier

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
    val rhm = new RHMClassifier(data, 10)
    val outliers = rhm.outliers
    val outliers2 = rhm.outliersAlt

    println(s"Outliers:    ${outliers.size}")
    println(s"Outliers2:   ${outliers2.size}")
    println(s"AllSubjects: ${data.columns.length-1}")
//    println(outliers.map(n => s"$n\n"))

  }
}
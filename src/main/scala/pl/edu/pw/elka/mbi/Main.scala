package pl.edu.pw.elka.mbi

import java.io.PrintWriter

import org.apache.spark.sql.{DataFrame, SparkSession}
import pl.edu.pw.elka.mbi.rhm.RHMClassifier

import scala.util.Random

object Main {

  // https://github.com/samtools/htsjdk/blob/master/src/test/java/htsjdk/tribble/index/tabix/TabixIndexTest.java
  // testQueryProvidedItemsAmount
  def main(args: Array[String]): Unit = {
    val iter = MultiFileIterator.openMultipleFiles(args.toList)
    val columns = iter.sortedColumnSet

    val cores = 4
    val spark = SparkSession.builder().appName("MBI").master(s"local[$cores]").getOrCreate()

    val laxAlgorithm = (rhm: RHMClassifier, data: DataFrame) => {if(data == null) rhm.findOutliers() else rhm.findOutliersFor(data)}
    val strictAlgorithm = (rhm: RHMClassifier, data: DataFrame) => {if(data == null) rhm.findOutliersStrict() else rhm.findOutliersStrictFor(data)}

    val testsFile = new PrintWriter(s"tests_${System.currentTimeMillis}.csv")
    testsFile.write(s"ReadLimit,RHMIterations,AlgorithmVariant,TrainingTime,TestingTime,ReferentialTime,TrainingOutliers,TestingOutliers,ReferentialOutliers,TotalVariants")

    Seq(
      (100, 10, laxAlgorithm),
      (1000, 10, laxAlgorithm),
      (10000, 10, laxAlgorithm),
      (100000, 10, laxAlgorithm),
      (100, 100, laxAlgorithm),
      (1000, 100, laxAlgorithm),
      (10000, 100, laxAlgorithm),
//      (100000, 100, laxAlgorithm)

      (100, 10, strictAlgorithm),
      (1000, 10, strictAlgorithm),
      (10000, 10, strictAlgorithm),
      (100000, 10, strictAlgorithm),
      (100, 100, strictAlgorithm),
      (1000, 100, strictAlgorithm),
      (10000, 100, strictAlgorithm)
//      (100000, 100, strictAlgorithm)

    ).foreach((testParams: (Int, Int, (RHMClassifier, DataFrame) => Set[String])) => {
      val readLimit = testParams._1
      val rhmIterations = testParams._2
      val rhmAlgorithm = testParams._3

      var timeVcfRead = System.currentTimeMillis
        val data = VcfSparkAdapter.createDataFrame(spark, columns, iter, readLimit)
      timeVcfRead = System.currentTimeMillis - timeVcfRead

      val trainDataColumns = Random.shuffle(data.columns.drop(1).toList).take(((data.columns.length-1)*0.7).toInt)
      val testDataColumns = data.columns.drop(1).toSet.filter(col => !trainDataColumns.contains(col)).toList
      val trainData = data.select("ID", trainDataColumns:_*)
      val testData = data.select("ID", testDataColumns:_*)

      var timeTraining = System.currentTimeMillis
        val rhm = new RHMClassifier(trainData, rhmIterations)
        val trainingOutliers = rhmAlgorithm(rhm, null)
      timeTraining = System.currentTimeMillis - timeTraining

      var timeTesting = System.currentTimeMillis
        val testingOutliers = rhmAlgorithm(rhm, testData)
      timeTesting = System.currentTimeMillis - timeTesting

      var timeReferential = System.currentTimeMillis
        val referencialRhm = new RHMClassifier(data, rhmIterations)
        val referencialOutliers = referencialRhm.findOutliers()
      timeReferential = System.currentTimeMillis - timeReferential

      testsFile.write(s"${readLimit},${rhmIterations},${if(rhmAlgorithm == laxAlgorithm) "lax" else "strict"},${timeTraining},${timeTesting},${timeReferential},${trainingOutliers.size},${testingOutliers.size},${referencialOutliers.size},${data.columns.length-1}")
      testsFile.flush()
    })

    testsFile.close()
  }
}
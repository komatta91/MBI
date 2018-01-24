package pl.edu.pw.elka.mbi

import java.io.PrintWriter

import org.apache.spark.sql.{DataFrame, SparkSession}
import pl.edu.pw.elka.mbi.rhm.RHMClassifier

import scala.util.Random

object Main {

  val laxAlgorithm = (rhm: RHMClassifier, data: DataFrame) => {if(data == null) rhm.findOutliers() else rhm.findOutliersFor(data)}
  val strictAlgorithm = (rhm: RHMClassifier, data: DataFrame) => {if(data == null) rhm.findOutliersStrict() else rhm.findOutliersStrictFor(data)}

  def main(args: Array[String]): Unit = {
    if(args.head.equalsIgnoreCase("test")){
      performanceTest(args.drop(1))
    }else if(args.head.equalsIgnoreCase("strict")){
      val rhmIterations = args(1).toInt
      run(args.drop(2)filter(path => !path.toLowerCase().endsWith(".vcf.gz.tbi")), strictAlgorithm, rhmIterations)
    }else if(args.head.equalsIgnoreCase("lax")){
      val rhmIterations = args(1).toInt
      run(args.drop(2)filter(path => !path.toLowerCase().endsWith(".vcf.gz.tbi")), laxAlgorithm, rhmIterations)
    }else{
      println("Expected arguments: [strict|lax] [RHM iterations] [*.vcf.gz] [*.vcf.gz] ...")
      println("Each *.vcf.gz file must have accompanying *.vcf.gz.tbi file in the same directory")
      println("[strict|lax] is the outlier detection algorithm variant")
      println("Results are written to the outliers_*.lst file")
    }
  }

  def run(args: Array[String], rhmAlgorithm: (RHMClassifier, DataFrame) => Set[String], rhmIterations: Int): Unit = {
    val iter = MultiFileIterator.openMultipleFiles(args.toList)
    val columns = iter.sortedColumnSet

    val cores = 4
    val spark = SparkSession.builder().appName("MBI").master(s"local[$cores]").getOrCreate()

    val outliersFile = new PrintWriter(s"outliers_${System.currentTimeMillis}.lst")


    val data = VcfSparkAdapter.createDataFrame(spark, columns, iter)

    val rhm = new RHMClassifier(data, rhmIterations)

    rhmAlgorithm(rhm, null).toSeq.sorted.foreach(outlier => outliersFile.write(s"$outlier\n"))

    outliersFile.close()
  }

  def performanceTest(args: Array[String]): Unit = {
    val iter = MultiFileIterator.openMultipleFiles(args.toList)
    val columns = iter.sortedColumnSet

    val cores = 4
    val spark = SparkSession.builder().appName("MBI").master(s"local[$cores]").getOrCreate()

    val testsFile = new PrintWriter(s"tests_${System.currentTimeMillis}.csv")
    testsFile.write(s"ReadLimit,RHMIterations,AlgorithmVariant,TrainingTime,TestingTime,ReferentialTime,TrainingOutliers,TestingOutliers,ReferentialOutliers,TotalVariants\n")

    Seq(
      (100, 2, laxAlgorithm),
      (1000, 2, laxAlgorithm),
      (10000, 2, laxAlgorithm),
      (100000, 2, laxAlgorithm),
      (100, 4, laxAlgorithm),
      (1000, 4, laxAlgorithm),
      (10000, 4, laxAlgorithm),

      (100, 2, strictAlgorithm),
      (1000, 2, strictAlgorithm),
      (10000, 2, strictAlgorithm),
      (100000, 2, strictAlgorithm),
      (100, 4, strictAlgorithm),
      (1000, 4, strictAlgorithm),
      (10000, 4, strictAlgorithm),

      (100000, 4, strictAlgorithm),
      (100000, 4, laxAlgorithm)

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
        val referencialOutliers = rhmAlgorithm(referencialRhm, null)
      timeReferential = System.currentTimeMillis - timeReferential

      testsFile.write(s"${readLimit},${rhmIterations},${if(rhmAlgorithm == laxAlgorithm) "lax" else "strict"},${timeTraining},${timeTesting},${timeReferential},${trainingOutliers.size},${testingOutliers.size},${referencialOutliers.size},${data.columns.length-1}\n")
      testsFile.flush()
    })

    testsFile.close()
  }
}
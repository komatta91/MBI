package pl.edu.pw.elka.mbi

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}

import scala.util.Random
import scala.collection.JavaConverters._

/**
  * Class representing single iteration of the Resampling by half means algorithm.
  * Half of the provided data is selected as the sampleData
  * @param iteration - iteration number
  * @param data - available data
  */
class RHMIteration (val iteration: Int, data: DataFrame) {
  val randomSubjects: List[String] = Random.shuffle(data.columns.drop(1).toList).take((data.columns.length-1)/2)
  val sampleData: DataFrame = data.select("ID", randomSubjects:_*)
  private val sampleSize: Int = sampleData.columns.length - 1
  private var alleleOccurrences: DataFrame = null
  private var alleleMeans: DataFrame = null
  private var alleleStandardDeviations: DataFrame = null

  /**
    * Counts allele occurrences on a given position represented by the row
    * @param row
    * @return map of the allele occurrences
    */
  private def calculateAlleleOccurrences(row: Row): Map[String, Any] = {
    Map("ID" -> row(0).toString) ++ //handle ID
    row.toSeq.toStream
      .drop(1) //skip already handled ID
      .groupBy(identity).mapValues(_.size) //count occurrences of a given allele
      .map(tuple => tuple._1.toString -> tuple._2) //construct occurrences map
  }

  /**
    * Transforms the sampleData DataFrame from [position, variant -> allele] to [position, allele -> occurrences]
    * @return transformed DataFrame
    */
  private def aggregateOccurrences(): Unit ={
    val occurrencesMap = sampleData.collect() //no way was found to move the process on Spark workers
      .map(calculateAlleleOccurrences)
    val schema = StructType(
      Seq(StructField("ID", StringType, nullable = false)) ++ //handle ID
      occurrencesMap.toStream
        .flatMap(entry => entry.keys.filter(key => !key.equals("ID")))
        .sorted
        .distinct
        .map(cn => StructField(cn, IntegerType, nullable = false))
    )
    val rows = occurrencesMap.toStream.map(
      entry => Row.fromSeq(
        Seq(entry("ID"))++
        schema.fields.toStream
          .filter(sf => !sf.name.equals("ID"))
          .sortWith((sf1, sf2) => sf1.name.compareTo(sf2.name) < 0)
          .map(field => {
            if (entry.keySet.contains(field.name)) {
              entry(field.name)
            }else{
              0
            }
          })
      )
    ).toList.asJava

    alleleOccurrences = sampleData.sparkSession.createDataFrame(rows, schema)
  }

  /**
    * Means calculation.
    * Because on each position every subject binarily may have or may not have a given allele,
    * the average is equal to the percentage of subjects having given allele on a given position.
    * @return calculated means DataFrame
    */
  def calculateMeans(): DataFrame = {
    if (alleleOccurrences == null) {
      aggregateOccurrences()
    }
    if (alleleMeans == null) {
      val schema = StructType(Seq(StructField("ID", StringType, nullable = false)) ++
        alleleOccurrences.schema.toStream.drop(1).map(sf => StructField(sf.name, DoubleType, nullable = false)))
      val rows = alleleOccurrences.collect.toStream
        .map(row => Row.fromSeq(Seq(row.get(0)) ++ row.toSeq.drop(1).map(value => value.toString.toDouble / sampleSize))).toList.asJava
      alleleMeans = sampleData.sparkSession.createDataFrame(rows, schema)
    }
    alleleMeans
  }

  /**
    * Standard deviation calculation.
    *
    * @return calculated standard deviations DataFrame
    */
  def calculateStandardDeviations(): DataFrame = {
    if(alleleOccurrences == null){
      aggregateOccurrences()
    }
    if (alleleStandardDeviations == null) {
      val schema = StructType(Seq(StructField("ID", StringType, nullable = false)) ++
        alleleOccurrences.schema.toStream.drop(1).map(sf => StructField(sf.name, DoubleType, nullable = false)))
      val rows = alleleOccurrences.collect.toStream
        .map(row => Row.fromSeq(Seq(row.get(0)) ++ row.toSeq.drop(1)
          .map(value => value.toString.toDouble)
          .map(value =>
            Math.sqrt(
              (value / sampleSize) * (sampleSize - value)
              )
            )
        )).toList.asJava
      alleleStandardDeviations = sampleData.sparkSession.createDataFrame(rows, schema)
    }
    alleleStandardDeviations
  }
}

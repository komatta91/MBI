package pl.edu.pw.elka.mbi

import org.apache.spark.sql.types.{StringType, IntegerType, StructField, StructType}
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
  val meanVector: DataFrame = calculateMeanVector()
  val standardDeviationVector: DataFrame = calculateStandardDeviationVector()
  private var alleleOccurrences: DataFrame = null

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

  private def calculateMeanVector(): DataFrame = {
    if(alleleOccurrences == null){
        aggregateOccurrences()
    }
    null
  }

  private def calculateStandardDeviationVector(): DataFrame = {
    if(alleleOccurrences == null){
      aggregateOccurrences()
    }
    null
  }
}

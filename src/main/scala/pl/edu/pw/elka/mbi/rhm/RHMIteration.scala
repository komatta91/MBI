package pl.edu.pw.elka.mbi.rhm

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.JavaConverters._
import scala.util.Random

/**
  * Class representing single iteration of the Resampling by half means algorithm.
  * Half of the provided data is selected as the sampleData
  * @param iteration - iteration number
  * @param data - available data
  */
class RHMIteration (val iteration: Int, val data: DataFrame) {
  lazy val sampleData: DataFrame = sampleData(data)
  lazy val distances: Map[String, Double] = calculateDistances()
  private lazy val sampleSize: Int = sampleData.columns.length - 1
  private lazy val scaledDataAlleleOccurrences: DataFrame = calculateScaledOccurrences()
  private lazy val sampleAlleleOccurrences: DataFrame = aggregateOccurrences(sampleData)
  private lazy val alleleMeans: DataFrame = calculateMeans()
  private lazy val alleleStandardDeviations: DataFrame = calculateStandardDeviations()
  private lazy val distancesOnPositions: DataFrame = calculateDistancesOnPositions()

  def sampleData(source: DataFrame): DataFrame ={
    val randomSubjects: List[String] = Random.shuffle(data.columns.drop(1).toList).take((data.columns.length-1)/2)
    data.select("ID", randomSubjects:_*)
  }

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
  def aggregateOccurrences(source: DataFrame): DataFrame ={
    val occurrencesMap = source.collect() //no way was found to move the process on Spark workers
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

    sampleData.sparkSession.createDataFrame(rows, schema)
  }

  /**
    * Means calculation.
    * Because on each position every subject binarily may have or may not have a given allele,
    * the average is equal to the percentage of subjects having given allele on a given position.
    * @return calculated means DataFrame
    */
  def calculateMeans(): DataFrame = {
    sampleSize

    val schema = StructType(Seq(StructField("ID", StringType, nullable = false)) ++
      sampleAlleleOccurrences.schema.toStream.drop(1).map(sf => StructField(sf.name, DoubleType, nullable = false)))
    val rows = sampleAlleleOccurrences.collect.toStream.par
      .map(row => Row.fromSeq(Seq(row.get(0)) ++ row.toSeq.drop(1).map(value => value.toString.toDouble / sampleSize))).toList.asJava
    sampleData.sparkSession.createDataFrame(rows, schema)
  }

  /**
    * Standard deviation calculation.
    *
    * @return calculated standard deviations DataFrame
    */
  def calculateStandardDeviations(): DataFrame = {
    val schema = StructType(Seq(StructField("ID", StringType, nullable = false)) ++
      sampleAlleleOccurrences.schema.toStream.drop(1).map(sf => StructField(sf.name, DoubleType, nullable = false)))
    val rows = sampleAlleleOccurrences.collect.toStream.par
      .map(row => Row.fromSeq(Seq(row.get(0)) ++ row.toSeq.drop(1)
        .map(value => value.toString.toDouble)
        .map(value =>
          Math.sqrt(
            (value / sampleSize) * (sampleSize - value)
            )
          )
      )).toList.asJava
    sampleData.sparkSession.createDataFrame(rows, schema)
  }

  /**
    * Scaling of the full dataset using means and standard deviations
    * @return expected occurrences of the alleles on positions
    */
  def calculateScaledOccurrences(): DataFrame = {
    sampleAlleleOccurrences
    alleleMeans
    alleleStandardDeviations

    val dataOccurences = aggregateOccurrences(data)
    val schema = StructType(Seq(StructField("ID", StringType, nullable = false)) ++
      dataOccurences.schema.toStream.drop(1).map(sf => StructField(sf.name, DoubleType, nullable = false)))
    val rows = dataOccurences.collect().toStream.par
      .map(row => {
        val id = row.get(0)
        val avgRow = alleleMeans.where(s"ID='$id'").first()
        val devRow = alleleStandardDeviations.where(s"ID='$id'").first()
        Row.fromSeq(
          Seq(id) ++
            schema.fieldNames.toStream
              .drop(1)
              .map(fn => {
                val avg = if(avgRow.schema.fieldNames.contains(fn)) avgRow.getAs[Double](fn) else 0d
                val dev = if(devRow.schema.fieldNames.contains(fn)) devRow.getAs[Double](fn) else 0d
                row.getAs[Int](fn).toDouble * avg + dev
            })
        )
      })
      .toList.asJava
    sampleData.sparkSession.createDataFrame(rows, schema)
  }

  /**
    * Euclidean distances calculation for each position
    * @return
    */
  def calculateDistancesOnPositions(): DataFrame ={
    scaledDataAlleleOccurrences

    val subjectsAmount = (data.columns.length - 1).toDouble
    val schema = StructType(Seq(StructField("ID", StringType, nullable = false)) ++
      data.schema.toStream.drop(1).map(sf => StructField(sf.name, DoubleType, nullable = false)))
    val rows = data.collect.toStream
      .map(row => {
        val id = row.get(0)

        val scaledRow = scaledDataAlleleOccurrences.where(s"ID='$id'").first()
        Row.fromSeq(
          Seq(id) ++
            1.until(row.length).toStream
              .map(i => row.getAs[String](i))
              .map(allele => subjectsAmount - scaledRow.getAs[Double](allele))
              .map(x => x)
        )
      })
      .toList.asJava
    sampleData.sparkSession.createDataFrame(rows, schema)
  }

  /**
    * Subjects Euclidean distances on all positions
    * @return
    */
  def calculateDistances(): Map[String, Double] = {
    (
      distancesOnPositions.columns.drop(1),
      distancesOnPositions.select(distancesOnPositions.columns(1), distancesOnPositions.columns.drop(2):_*)
      .reduce((acc, row) => Row.fromSeq((acc.toSeq.map(_.asInstanceOf[Double]), row.toSeq.map(_.asInstanceOf[Double])).zipped.map(_+_)))
      .toSeq
      .map(_.asInstanceOf[Double])
    ).zipped.toMap
  }
}

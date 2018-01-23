package pl.edu.pw.elka.mbi.rhm

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.junit.Assert._
import org.junit.{Before, Test}
import org.scalatest.junit.AssertionsForJUnit

import scala.collection.JavaConverters._

class RHMClassifierSuite extends AssertionsForJUnit {

  var trainDataFrame: DataFrame = _
  var testDataFrame: DataFrame = _
  var outlierDataFrame: DataFrame = _

  @Before def initialize() {
    val spark = SparkSession.builder().appName("MBI").master("local[2]").getOrCreate()
    val trainSchema = StructType(Seq(
      StructField("ID", StringType, nullable = false),
      StructField("Sub1", StringType, nullable = false),
      StructField("Sub2", StringType, nullable = false),
      StructField("Sub3", StringType, nullable = false),
      StructField("Sub4", StringType, nullable = false)
    ))
    trainDataFrame = spark.createDataFrame(Seq(
      Row.fromTuple(("pos1", "A", "A", "A", "A")),
      Row.fromTuple(("pos2", "A", "A", "B", "A")),
      Row.fromTuple(("pos3", "B", "B", "A", "A")),
      Row.fromTuple(("pos4", "A", "B", "C", "D")),
      Row.fromTuple(("pos5", "A", "B", "C", "A"))
    ).toList.asJava, trainSchema)

    val testSchema = StructType(Seq(
      StructField("ID", StringType, nullable = false),
      StructField("Sub5", StringType, nullable = false),
      StructField("Sub6", StringType, nullable = false),
      StructField("Sub7", StringType, nullable = false),
      StructField("Sub8", StringType, nullable = false),
      StructField("Sub9", StringType, nullable = false)
    ))
    testDataFrame = spark.createDataFrame(Seq(
      Row.fromTuple(("pos1", "B", "A", "A", "A", "Z")),
      Row.fromTuple(("pos2", "B", "A", "A", "A", "Z")),
      Row.fromTuple(("pos3", "B", "A", "B", "A", "Z")),
      Row.fromTuple(("pos4", "C", "D", "A", "B", "Z")),
      Row.fromTuple(("pos5", "C", "A", "A", "B", "Z"))
    ).toList.asJava, testSchema)

    val outlierSchema = StructType(Seq(
      StructField("ID", StringType, nullable = false),
      StructField("Outlier", StringType, nullable = false)
    ))
    outlierDataFrame = spark.createDataFrame(Seq(
      Row.fromTuple(("pos1", "Z")),
      Row.fromTuple(("pos2", "Z")),
      Row.fromTuple(("pos3", "Z")),
      Row.fromTuple(("pos4", "Z")),
      Row.fromTuple(("pos5", "Z"))
    ).toList.asJava, outlierSchema)
  }

  @Test def verifyTrainCalculateAverageDistances() {
    val res = new RHMClassifier(trainDataFrame, 30).calculateAverageDistances()
    val delta = 1

    assertEquals( 9.76, res("Sub1"), delta)
    assertEquals(10.65, res("Sub2"), delta)
    assertEquals(12.65, res("Sub3"), delta)
    assertEquals( 9.76, res("Sub4"), delta)
  }

  @Test def verifyTrainFindOutliersStrict() {
    val res = new RHMClassifier(trainDataFrame, 30).findOutliersStrict()

    assertEquals(Set(), res)
  }

  @Test def verifyTrainFindOutliers() {
    val res = new RHMClassifier(trainDataFrame, 90).findOutliers()

    assertEquals(Set("Sub3"), res)
  }

  @Test def verifyTestCalculateAverageDistances() {
    val res = new RHMClassifier(trainDataFrame, 30).calculateAverageDistancesFor(testDataFrame)
    val delta = 1

    assertEquals( 9.76, res("Sub1"), delta)
    assertEquals(10.65, res("Sub2"), delta)
    assertEquals(12.65, res("Sub3"), delta)
    assertEquals( 9.76, res("Sub4"), delta)

    assertEquals(41.67, res("Sub5"), delta)
    assertEquals(34.91, res("Sub6"), delta)
    assertEquals(34.99, res("Sub7"), delta)
    assertEquals(35.51, res("Sub8"), delta)
    assertEquals(45.00, res("Sub9"), delta)
  }

  @Test def verifyTestFindOutliersStrict() {
    val res = new RHMClassifier(trainDataFrame, 90).findOutliersStrictFor(testDataFrame)

    assertEquals(Set(), res)
  }

  @Test def verifyTestFindOutliers() {
    val res = new RHMClassifier(trainDataFrame, 30).findOutliersFor(testDataFrame)

    assertEquals(Set("Sub1", "Sub2", "Sub3", "Sub4"), res)
  }


  @Test def verifySingleOutlierCalculateAverageDistances() {
    val res = new RHMClassifier(trainDataFrame, 30).calculateAverageDistancesFor(outlierDataFrame)
    val delta = 1

    assertEquals( 9.76, res("Sub1"), delta)
    assertEquals(10.65, res("Sub2"), delta)
    assertEquals(12.65, res("Sub3"), delta)
    assertEquals( 9.76, res("Sub4"), delta)

    assertEquals(25.00, res("Outlier"), delta)
  }

  @Test def verifySingleOutlierTestFindOutliersStrict() {
    val res = new RHMClassifier(trainDataFrame, 30).findOutliersStrictFor(outlierDataFrame)

    assertEquals(Set("Outlier"), res)
  }

  @Test def verifySingleOutlierTestFindOutliers() {
    val res = new RHMClassifier(trainDataFrame, 30).findOutliersFor(outlierDataFrame)

    assertEquals(Set("Outlier", "Sub3"), res)
  }
}

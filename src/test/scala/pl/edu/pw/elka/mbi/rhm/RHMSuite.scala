package pl.edu.pw.elka.mbi.rhm

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.junit.Assert._
import org.junit.{Before, Test}
import org.scalatest.junit.AssertionsForJUnit

import scala.collection.JavaConverters._

class RHMSuite extends AssertionsForJUnit {

  var testDataFrame: DataFrame = _

  @Before def initialize() {
    val spark = SparkSession.builder().appName("MBI").master("local[2]").getOrCreate()
    val schema = StructType(Seq(
      StructField("ID", StringType, nullable = false),
      StructField("Sub1", StringType, nullable = false),
      StructField("Sub2", StringType, nullable = false),
      StructField("Sub3", StringType, nullable = false),
      StructField("Sub4", StringType, nullable = false)
    ))
    testDataFrame = spark.createDataFrame(Seq(
      Row.fromTuple(("pos1", "A", "A", "A", "A")),
      Row.fromTuple(("pos2", "A", "A", "B", "A")),
      Row.fromTuple(("pos3", "B", "B", "A", "A")),
      Row.fromTuple(("pos4", "A", "B", "C", "D")),
      Row.fromTuple(("pos5", "A", "B", "C", "A"))
    ).toList.asJava, schema)
  }

  @Test def verifyCalculateAverageDistances() {
    val res = new RHM(testDataFrame, 30).calculateAverageDistances()
    val delta = 1

    assertEquals( 9.76, res("Sub1"), delta)
    assertEquals(10.65, res("Sub2"), delta)
    assertEquals(12.65, res("Sub3"), delta)
    assertEquals( 9.76, res("Sub4"), delta)
  }

  @Test def verifyFindOutliersStrict() {
    val res = new RHM(testDataFrame, 30).findOutliersStrict()

    assertEquals(Set(), res)
  }

  @Test def verifyFindOutliers() {
    val res = new RHM(testDataFrame, 30).findOutliers()

    assertEquals(Set("Sub3"), res)
  }
}

package pl.edu.pw.elka.mbi.rhm

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.junit.Assert._
import org.junit.{Before, Test}
import org.scalatest.junit.AssertionsForJUnit

import scala.collection.JavaConverters._

class RHMIterationSuite extends AssertionsForJUnit {

  var testDataFrame: DataFrame = _

  private def createDeterministicRHMIteration(): RHMIteration ={
    new RHMIteration(0, testDataFrame){
      override def sampleData(source: DataFrame): DataFrame = {
        source //prevent halving the data
      }
    }
  }

  @Before def initialize() {
    val spark = SparkSession.builder().appName("MBI").master("local[1]").getOrCreate()
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

  @Test def verifyAggregateOccurrences() {
    val res = createDeterministicRHMIteration().aggregateOccurrences(testDataFrame)

    assertArrayEquals(Array(4, 0, 0, 0), res.where("ID='pos1'").first().toSeq.drop(1).map(_.toString.toInt).toArray)
    assertArrayEquals(Array(3, 1, 0, 0), res.where("ID='pos2'").first().toSeq.drop(1).map(_.toString.toInt).toArray)
    assertArrayEquals(Array(2, 2, 0, 0), res.where("ID='pos3'").first().toSeq.drop(1).map(_.toString.toInt).toArray)
    assertArrayEquals(Array(1, 1, 1, 1), res.where("ID='pos4'").first().toSeq.drop(1).map(_.toString.toInt).toArray)
    assertArrayEquals(Array(2, 1, 1, 0), res.where("ID='pos5'").first().toSeq.drop(1).map(_.toString.toInt).toArray)
  }

  @Test def verifyCalculateMeans() {
    val res = createDeterministicRHMIteration().calculateMeans()
    val delta = 0.0001

    assertArrayEquals(Array(1.0,  0,    0,    0   ), res.where("ID='pos1'").first().toSeq.drop(1).map(_.toString.toDouble).toArray, delta)
    assertArrayEquals(Array(0.75, 0.25, 0,    0   ), res.where("ID='pos2'").first().toSeq.drop(1).map(_.toString.toDouble).toArray, delta)
    assertArrayEquals(Array(0.5,  0.5,  0,    0   ), res.where("ID='pos3'").first().toSeq.drop(1).map(_.toString.toDouble).toArray, delta)
    assertArrayEquals(Array(0.25, 0.25, 0.25, 0.25), res.where("ID='pos4'").first().toSeq.drop(1).map(_.toString.toDouble).toArray, delta)
    assertArrayEquals(Array(0.5,  0.25, 0.25, 0   ), res.where("ID='pos5'").first().toSeq.drop(1).map(_.toString.toDouble).toArray, delta)
  }

  @Test def verifyCalculateStandardDeviations() {
    val res = createDeterministicRHMIteration().calculateStandardDeviations()
    val delta = 0.01

    assertArrayEquals(Array(0d,   0,    0,    0   ), res.where("ID='pos1'").first().toSeq.drop(1).map(_.toString.toDouble).toArray, delta)
    assertArrayEquals(Array(0.86, 0.86, 0,    0   ), res.where("ID='pos2'").first().toSeq.drop(1).map(_.toString.toDouble).toArray, delta)
    assertArrayEquals(Array(1.0,  1.0,  0,    0   ), res.where("ID='pos3'").first().toSeq.drop(1).map(_.toString.toDouble).toArray, delta)
    assertArrayEquals(Array(0.86, 0.86, 0.86, 0.86), res.where("ID='pos4'").first().toSeq.drop(1).map(_.toString.toDouble).toArray, delta)
    assertArrayEquals(Array(1.0,  0.86, 0.86, 0   ), res.where("ID='pos5'").first().toSeq.drop(1).map(_.toString.toDouble).toArray, delta)
  }

  @Test def verifyCalculateScaledOccurrences() {
    val res = createDeterministicRHMIteration().calculateScaledOccurrences()
    val delta = 0.14

    assertArrayEquals(Array(4.0, 0, 0, 0), res.where("ID='pos1'").first().toSeq.drop(1).map(_.toString.toDouble).toArray, delta)
    assertArrayEquals(Array(3.0, 1, 0, 0), res.where("ID='pos2'").first().toSeq.drop(1).map(_.toString.toDouble).toArray, delta)
    assertArrayEquals(Array(2.0, 2, 0, 0), res.where("ID='pos3'").first().toSeq.drop(1).map(_.toString.toDouble).toArray, delta)
    assertArrayEquals(Array(1.0, 1, 1, 1), res.where("ID='pos4'").first().toSeq.drop(1).map(_.toString.toDouble).toArray, delta)
    assertArrayEquals(Array(2.0, 1, 1, 0), res.where("ID='pos5'").first().toSeq.drop(1).map(_.toString.toDouble).toArray, delta)
  }

  @Test def verifyCalculateDistancesOnPositions() {
    val res = createDeterministicRHMIteration().calculateDistancesOnPositions()
    val delta = 0.01

    assertArrayEquals(Array(0.0,  0,    0,    0   ), res.where("ID='pos1'").first().toSeq.drop(1).map(_.toString.toDouble).toArray, delta)
    assertArrayEquals(Array(0.88, 0.88, 2.88, 0.88), res.where("ID='pos2'").first().toSeq.drop(1).map(_.toString.toDouble).toArray, delta)
    assertArrayEquals(Array(2.0,  2,    2,    2   ), res.where("ID='pos3'").first().toSeq.drop(1).map(_.toString.toDouble).toArray, delta)
    assertArrayEquals(Array(2.88, 2.88, 2.88, 2.88), res.where("ID='pos4'").first().toSeq.drop(1).map(_.toString.toDouble).toArray, delta)
    assertArrayEquals(Array(2.0,  2.88, 2.88, 2   ), res.where("ID='pos5'").first().toSeq.drop(1).map(_.toString.toDouble).toArray, delta)
  }

  @Test def verifyCalculateDistances() {
    val res = createDeterministicRHMIteration().calculateDistances()
    val delta = 0.01

    assertEquals( 7.76, res("Sub1"), delta)
    assertEquals( 8.65, res("Sub2"), delta)
    assertEquals(10.65, res("Sub3"), delta)
    assertEquals( 7.76, res("Sub4"), delta)
  }
}

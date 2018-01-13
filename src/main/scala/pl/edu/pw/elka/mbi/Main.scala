package pl.edu.pw.elka.mbi

import java.io.File

import htsjdk.samtools.util.CloseableIterator
import htsjdk.variant.variantcontext.{GenotypesContext, VariantContext}
import htsjdk.variant.vcf.VCFFileReader
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer


object Main {

  private def openMultipleFiles(vcfs: List[String]): (List[String], CloseableIterator[VariantContext]) = {
    val iteratorList = ListBuffer[CloseableIterator[VariantContext]]()
    var columnSet = Set[String]()
    for(vcf <- vcfs){
      val data = new File(vcf)
      val tabix = new File(vcf + ".tbi")
      val fileReader = new VCFFileReader(data, tabix, true)
      iteratorList += fileReader.iterator
      columnSet ++= fileReader.getFileHeader.getGenotypeSamples.asScala
    }
    (columnSet.toList.sorted, new CloseableIterator[VariantContext]{
      private val iteratorQueue = iteratorList
      private var currentIterator: CloseableIterator[VariantContext] = iteratorList.head
      private var currentIteratorIdx = 0

      private def rollIterators(): Unit ={
        while(!currentIterator.hasNext && currentIteratorIdx+1 < iteratorQueue.size){
          currentIteratorIdx += 1
          currentIterator = iteratorList(currentIteratorIdx)
        }
      }

      override def close(): Unit = {
        iteratorQueue.foreach(x => x.close())
      }

      override def next(): VariantContext = {
        rollIterators()
        currentIterator.next()//.fullyDecode(headerQueue.get(currentIteratorIdx), true)
      }

      override def hasNext: Boolean = {
        rollIterators()
        currentIterator.hasNext
      }
    })
  }

  private def adaptToSpark(spark: SparkSession, schema: StructType, iter: CloseableIterator[VariantContext], snpLimit: Int): DataFrame = {
    val makeRow = (variantName: String, genotypes: GenotypesContext) => {
      Row.fromSeq(schema.map(x => x.name).map {
        case "ID" => variantName
        case c => Option(genotypes.get(c).getGenotypeString).getOrElse("")
      })
    }
    val rowList: java.util.List[Row] = iter.asScala.toStream
      .filter(variant => variant.isSNP)
      .take(snpLimit)
      .filter(variant => variant.getGenotypes.asScala.map(genotype => genotype.getGenotypeString).toStream.distinct.size > 1)
      .map(variant => makeRow(s"${variant.getContig}:${variant.getID}", variant.getGenotypes))
      .asJava
    spark.createDataFrame(rowList, schema)
  }


  // https://github.com/samtools/htsjdk/blob/master/src/test/java/htsjdk/tribble/index/tabix/TabixIndexTest.java
  // testQueryProvidedItemsAmount
  def main(args: Array[String]): Unit = {
    val (columns, iter) = openMultipleFiles(args.toList)

    val spark = SparkSession.builder().appName("MBI").master("local[4]").getOrCreate()
    val schema = StructType((List("ID") ++ columns).map(cn => StructField(cn, StringType, nullable = true)))
//    var data = spark.createDataFrame(spark.emptyDataFrame.rdd, schema)

    val startMillis = System.currentTimeMillis
    val data = adaptToSpark(spark, schema, iter, 100)
    println((System.currentTimeMillis - startMillis)+"ms")
    println(data.count())
  }
}
package pl.edu.pw.elka.mbi

import htsjdk.samtools.util.CloseableIterator
import htsjdk.variant.variantcontext.{GenotypesContext, VariantContext}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.JavaConverters._


object VcfSparkAdapter {

  /**
    * Adapter method creating a Spark DataFrame from the htsjdk library CloseableIterator[VariantContext] from the VCF files.
    * All non-SNP variants are omitted.
    * @param sparkSession - SparkSession instance
    * @param columns - list of column names from VCF files
    * @param vcfIterator - VCF files iterator
    * @param snpLimit - limit of variants read from VCF files
    * @return - new spark session DataFrame
    */
  def createDataFrame(sparkSession: SparkSession, columns: List[String], vcfIterator: CloseableIterator[VariantContext], snpLimit: Int): DataFrame = {
    val schema = StructType((List("ID") ++ columns).map(cn => StructField(cn, StringType, nullable = true)))

    val makeRow = (variantName: String, genotypes: GenotypesContext) => {
      Row.fromSeq(schema.map(x => x.name).map {
        case "ID" => variantName
        case c => Option(genotypes.get(c).getGenotypeString).getOrElse("")
      })
    }

    val rowList: java.util.List[Row] = vcfIterator.asScala.toStream
      .filter(variant => variant.isSNP)
      .take(snpLimit)
      .filter(variant => variant.getGenotypes.asScala.map(genotype => genotype.getGenotypeString).toStream.distinct.size > 1)
      .map(variant => makeRow(s"${variant.getContig}:${variant.getID}", variant.getGenotypes))
      .asJava

    sparkSession.createDataFrame(rowList, schema)
  }
}

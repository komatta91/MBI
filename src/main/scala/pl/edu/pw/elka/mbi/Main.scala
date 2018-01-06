package pl.edu.pw.elka.mbi

import java.io.File
import java.util
import java.util.stream.StreamSupport

import com.google.common.collect.{HashBasedTable, Table}
import htsjdk.samtools.util.CloseableIterator
import htsjdk.variant.variantcontext.VariantContext
import htsjdk.variant.vcf.VCFFileReader

object Main {

  private def openMultipleFiles(vcfs: List[String]): CloseableIterator[VariantContext] = {
    val iteratorList = new util.ArrayList[CloseableIterator[VariantContext]]()
    //val headerList = new util.ArrayList[VCFHeader]()
    for(vcf <- vcfs){
      val data = new File(vcf)
      val tabix = new File(vcf + ".tbi")
      val fileReader = new VCFFileReader(data, tabix, true)
      iteratorList.add(fileReader.iterator)
      //headerList.add(fileReader.getFileHeader)
    }
    new CloseableIterator[VariantContext]{
      private val iteratorQueue = iteratorList
      //private val headerQueue = headerList
      private var currentIterator: CloseableIterator[VariantContext] = iteratorList.get(0)
      private var currentIteratorIdx = 0

      private def rollIterators(): Unit ={
        while(!currentIterator.hasNext && currentIteratorIdx+1 < iteratorQueue.size()){
          currentIteratorIdx += 1
          currentIterator = iteratorList.get(currentIteratorIdx)
        }
      }

      override def close(): Unit = {
        iteratorQueue.forEach(x => x.close())
      }

      override def next(): VariantContext = {
        rollIterators()
        currentIterator.next()//.fullyDecode(headerQueue.get(currentIteratorIdx), true)
      }

      override def hasNext: Boolean = {
        rollIterators()
        currentIterator.hasNext
      }
    }
  }

  private def transformToTable(iter: CloseableIterator[VariantContext], snpLimit: Int): Table[String, String, String] = {
    val table: Table[String, String, String] = HashBasedTable.create()
    val syncPut = (r: String, c: String, v: String) => {
      table.synchronized{
        table.put(r, c, v)
      }
    }
    iter.stream()
      .parallel()
      .filter(variant => variant.isSNP)
      .limit(snpLimit)
      .filter(variant => StreamSupport.stream(variant.getGenotypesOrderedByName.spliterator, true)
        .map[String](genotype => genotype.getGenotypeString)
        .distinct
        .count > 1)
      .sequential() // necessary - Variant/Genotype classes are not thread-safe
      .forEach(variant => variant.getGenotypesOrderedByName.forEach(g => syncPut(s"${variant.getContig}:${variant.getID}", g.getSampleName, g.getGenotypeString)))

    table
  }


  // https://github.com/samtools/htsjdk/blob/master/src/test/java/htsjdk/tribble/index/tabix/TabixIndexTest.java
  // testQueryProvidedItemsAmount
  def main(args: Array[String]): Unit = {
    val iter = openMultipleFiles(args.toList)
    val startMillis = System.currentTimeMillis
    val table1 = transformToTable(iter, 1000)
    val table2 = transformToTable(iter, 1000)
    println((System.currentTimeMillis - startMillis)+"ms")
    println(table1.size())
    println(table2.size())
  }
}
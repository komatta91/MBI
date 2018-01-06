package pl.edu.pw.elka.mbi

import java.io.File
import java.util

import htsjdk.samtools.util.CloseableIterator
import htsjdk.variant.variantcontext.VariantContext
import htsjdk.variant.vcf.VCFFileReader

object Main {

  def openMultipleFiles(vcfs: List[String], query: String): CloseableIterator[VariantContext] = {
    var iteratorList = new util.ArrayList[CloseableIterator[VariantContext]]()
    for(vcf <- vcfs){
      val data = new File(vcf)
      val tabix = new File(vcf + ".tbi")
      iteratorList.add(new VCFFileReader(data, tabix, true).iterator()) //todo query?
    }
    new CloseableIterator[VariantContext]{
      private val iteratorQueue = iteratorList
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
        currentIterator.next()
      }

      override def hasNext: Boolean = {
        rollIterators()
        currentIterator.hasNext
      }
    }
  }


  // https://github.com/samtools/htsjdk/blob/master/src/test/java/htsjdk/tribble/index/tabix/TabixIndexTest.java
  // testQueryProvidedItemsAmount
  def main(args: Array[String]): Unit = {
    val iter = openMultipleFiles(args.toList, "")

    iter.forEachRemaining(x => println(x))
  }
}
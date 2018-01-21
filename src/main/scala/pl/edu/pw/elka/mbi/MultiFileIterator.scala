package pl.edu.pw.elka.mbi

import java.io.File

import htsjdk.samtools.util.CloseableIterator
import htsjdk.variant.variantcontext.VariantContext
import htsjdk.variant.vcf.VCFFileReader

import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._

/**
  * Extension of the VCFFileReader utility from htsjdk library.
  * Provides transparent, sequential reading of multiple VCF files with their tabbix'es as a single CloseableIterator[VariantContext].
  * @param iteratorList - list of CloseableIterator[VariantContext] objects from VCFFileReaders
  * @param columnSet - sorted list of columns available in VCFFileReaders
  */
class MultiFileIterator private(iteratorList: ListBuffer[CloseableIterator[VariantContext]], columnSet: List[String])
  extends CloseableIterator[VariantContext] {
  val sortedColumnSet: List[String] = columnSet
  private val iteratorQueue = iteratorList
  private var currentIterator: CloseableIterator[VariantContext] = iteratorList.head
  private var currentIteratorIdx = 0

  private def rollIterators(): Unit ={
    while(!currentIterator.hasNext && currentIteratorIdx+1 < iteratorQueue.size){
      currentIteratorIdx += 1
      currentIterator = iteratorQueue(currentIteratorIdx)
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
}

/**
  * Companion object providing the factory method for MultiFileIterator
  */
object MultiFileIterator {

  /**
    * Factory method constructing MultiFileIterator object.
    * All provided VCF files must be accompanied by their tabbix (*.tbi) files with the same name.
    * For abc.vcf file there must also be abc.vcf.tbi file present in the same directory.
    * @param vcfs - VCF files to be opened (not the tabbixes)
    * @return new MultiFileIterator
    */
  def openMultipleFiles(vcfs: List[String]): MultiFileIterator = {
    val iteratorList = ListBuffer[CloseableIterator[VariantContext]]()
    var columnSet = Set[String]()
    for (vcf <- vcfs) {
      val data = new File(vcf)
      val tabix = new File(vcf + ".tbi")
      val fileReader = new VCFFileReader(data, tabix, true)
      iteratorList += fileReader.iterator
      columnSet ++= fileReader.getFileHeader.getGenotypeSamples.asScala
    }
    new MultiFileIterator(iteratorList, columnSet.toList.sorted)
  }
}

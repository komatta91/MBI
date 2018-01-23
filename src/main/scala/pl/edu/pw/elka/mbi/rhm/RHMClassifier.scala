package pl.edu.pw.elka.mbi.rhm

import org.apache.spark.sql.DataFrame

/**
  * Implementation of the RHM algorithm based outlier detection
  * @param iterations number of iterations to perform
  * @param trainData training dataset
  */
class RHMClassifier(trainData: DataFrame, val iterations: Int) {

  lazy val outliers: Set[String] = findOutliersStrict()
  lazy val outliersAlt: Set[String] = findOutliers()
  private lazy val iterationSeq: Seq[RHMIteration] = 0.until(iterations).map(i => new RHMIteration(i, trainData))
  private lazy val averageDistances: Map[String, Double] = calculateAverageDistances()

  /**
    * Method calculating average distances from the RHMIteration distances
    * @return map of the subject average distances
    */
  def calculateAverageDistances(): Map[String, Double] = {
    iterationSeq.map(iter => iter.trainDistances)
      .reduce((acc, m) => acc ++ m.map { case (k,v) => k -> (v + acc.getOrElse(k,0.0)) })
      .map((entry: (String, Double)) => entry._1 -> entry._2 / iterations)
  }

  /**
    * Method calculating average distances for given subjects
    * @return map of the subject average distances
    */
  def calculateAverageDistancesFor(subjects: DataFrame): Map[String, Double] = {
    iterationSeq.map(iter => iter.calculateDistanceFor(
      iter.calculateDistancesOnPositionsFor(subjects).join(iter.distancesOnPositions, "ID")
    ))
      .reduce((acc, m) => acc ++ m.map { case (k,v) => k -> (v + acc.getOrElse(k,0.0)) })
      .map((entry: (String, Double)) => entry._1 -> entry._2 / iterations)
  }

  /**
    * Method finding outliers.
    * Calculated average distance of the outlier is greater than 1.4286 times average distance median.
    * Based on the performed test's, this method is more strict than the findOutliers() method.
    * @return set of the outliers
    */
  def findOutliersStrict(): Set[String] = {
    val median = averageDistances.values.toList.sorted.apply(averageDistances.size / 2)
    averageDistances.filter((entry: (String, Double)) => entry._2 > 1.4286 * median).keySet
  }

  /**
    * Method finding outliers from given subjects.
    * Calculated average distance of the outlier is greater than 1.4286 times average distance median.
    * Based on the performed test's, this method is more strict than the findOutliers() method.
    * @return set of the outliers
    */
  def findOutliersStrictFor(subjects: DataFrame): Set[String] = {
    val subjectDistances = calculateAverageDistancesFor(subjects)
    val median = subjectDistances.values.toList.sorted.apply(subjectDistances.size / 2)
    subjectDistances.filter((entry: (String, Double)) => entry._2 > 1.4286 * median).keySet.intersect(subjectDistances.keySet)
  }

  /**
    * Alternative method finding outliers.
    * Calculated average distance of the outlier difference from the median of those distances is greater than 1.4286 times median of those differences.
    * Based on the performed test's, this method is more lax than the findOutliersStrict() method.
    * @return set of the outliers
    */
  def findOutliers(): Set[String] = {
    val median = averageDistances.values.toList.sorted.apply(averageDistances.size / 2)
    val cutoff = 1.4286 * averageDistances.values.map(dist => Math.abs(dist - median)).toList.sorted.apply(averageDistances.size / 2)
    averageDistances.filter((entry: (String, Double)) => Math.abs(entry._2 - median) > cutoff).keySet
  }

  /**
    * Alternative method finding outliers from given subjects.
    * Calculated average distance of the outlier difference from the median of those distances is greater than 1.4286 times median of those differences.
    * Based on the performed test's, this method is more lax than the findOutliersStrict() method.
    * @return set of the outliers
    */
  def findOutliersFor(subjects: DataFrame): Set[String] = {
    val subjectDistances = calculateAverageDistancesFor(subjects)
    val median = subjectDistances.values.toList.sorted.apply(subjectDistances.size / 2)
    val cutoff = 1.4286 * subjectDistances.values.map(dist => Math.abs(dist - median)).toList.sorted.apply(subjectDistances.size / 2)
    subjectDistances.filter((entry: (String, Double)) => Math.abs(entry._2 - median) > cutoff).keySet.intersect(subjectDistances.keySet)
  }
}

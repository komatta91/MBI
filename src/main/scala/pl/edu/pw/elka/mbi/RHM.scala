package pl.edu.pw.elka.mbi

import org.apache.spark.sql.DataFrame

/**
  * Implementation of the RHM algorithm based outlier detection
  * @param iterations
  * @param data
  */
class RHM (data: DataFrame, val iterations: Int) {

  lazy val outliers: Set[String] = findOutliers()
  lazy val outliersAlt: Set[String] = findOutliersAlt()
  private lazy val averageDistances: Map[String, Double] = calculateAverageDistances()

  /**
    * Method executing RHMIterations
    * @return sequence of the RHMIteration results
    */
  private def executeIterations(): Seq[Map[String, Double]] = {
    0.until(iterations).map(i => new RHMIteration(i, data).distances)
  }

  /**
    * Method calculating average distances from the RHMIteration distances
    * @return map of the subject average distances
    */
  private def calculateAverageDistances(): Map[String, Double] = {
    executeIterations().reduce((acc, m) => acc ++ m.map { case (k,v) => k -> (v + acc.getOrElse(k,0.0)) })
  }

  /**
    * Method finding outliers.
    * Calculated average distance of the outlier is greater than 1.4286 times average distance median.
    * @return set of the outliers
    */
  private def findOutliers(): Set[String] = {
    val median = averageDistances.values.toList.sorted.apply(averageDistances.size / 2)
    averageDistances.filter((entry: (String, Double)) => entry._2 > 1.4286 * median).keySet
  }

  /**
    * Alternative method finding outliers.
    * Calculated average distance of the outlier difference from the median of those distances is greater than 1.4286 times median of those differences.
    * Based on the performed test's, this method is more lax than the findOutliers() method.
    * @return set of the outliers
    */
  private def findOutliersAlt(): Set[String] = {
    val median = averageDistances.values.toList.sorted.apply(averageDistances.size / 2)
    val cutoff = 1.4286 * averageDistances.values.map(dist => Math.abs(dist - median)).toList.sorted.apply(averageDistances.size / 2)
    averageDistances.filter((entry: (String, Double)) => Math.abs(entry._2 - median) > cutoff).keySet
  }

}

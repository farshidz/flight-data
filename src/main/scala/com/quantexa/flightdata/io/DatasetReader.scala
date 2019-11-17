package com.quantexa.flightdata.io

import org.apache.spark.sql.Dataset

/**
  * Implemented by objects that read [[Dataset]]s.
  *
  * @tparam T Type of [[Dataset]]. Either a case class or [[org.apache.spark.sql.Row]] for a generic
  *           [[org.apache.spark.sql.DataFrame]]
  */
trait DatasetReader[T] {
  /**
    * Loads a [[Dataset]].
    */
  def getDataset: Dataset[T]
}
